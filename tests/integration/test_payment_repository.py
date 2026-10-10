import asyncio
from datetime import timedelta
from decimal import Decimal
from uuid import uuid7

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from app.domain.enums import Currency, PaymentStatus
from app.domain.exceptions import InvalidStatusTransition
from app.domain.payment import Payment
from app.infrastructure.db.models import PaymentModel
from tests.integration.support import NOW, UowFactory, make_payment

pytestmark = pytest.mark.integration


async def _save(uow_factory: UowFactory, payment: Payment) -> None:
    async with uow_factory() as uow:
        assert await uow.payments.insert_if_absent(payment) is True
        await uow.commit()


async def _load(uow_factory: UowFactory, payment: Payment) -> Payment:
    async with uow_factory() as uow:
        loaded = await uow.payments.get_by_id(payment.id)
    assert loaded is not None
    return loaded


async def test_insert_if_absent_inserts_only_once_per_idempotency_key(
    uow_factory: UowFactory, engine: AsyncEngine
) -> None:
    first = make_payment("key-1")
    duplicate = make_payment("key-1")  # same key, new id
    assert first.id != duplicate.id

    async with uow_factory() as uow:
        assert await uow.payments.insert_if_absent(first) is True
        assert await uow.payments.insert_if_absent(duplicate) is False
        await uow.commit()

    async with engine.connect() as conn:
        rows = (await conn.execute(select(PaymentModel.id))).scalars().all()
    assert rows == [first.id]


async def test_get_by_id_round_trip(uow_factory: UowFactory) -> None:
    payment = make_payment(
        amount=Decimal("100"),
        currency=Currency.USD,
        metadata={"order_id": 42, "nested": {"ru": "Привет", "ok": True}, "list": [1, None]},
    )
    await _save(uow_factory, payment)

    loaded = await _load(uow_factory, payment)

    assert loaded == payment
    assert str(loaded.amount) == "100.00"
    assert type(loaded.currency) is Currency
    assert type(loaded.status) is PaymentStatus
    assert loaded.status is PaymentStatus.PENDING
    assert loaded.created_at == NOW
    assert loaded.created_at.utcoffset() == timedelta(0)
    assert loaded.processed_at is None
    assert loaded.webhook_sent_at is None


async def test_get_by_id_unknown_returns_none(uow_factory: UowFactory) -> None:
    async with uow_factory() as uow:
        assert await uow.payments.get_by_id(uuid7()) is None


async def test_get_by_idempotency_key(uow_factory: UowFactory) -> None:
    payment = make_payment("key-abc")
    await _save(uow_factory, payment)

    async with uow_factory() as uow:
        assert await uow.payments.get_by_idempotency_key("key-abc") == payment
        assert await uow.payments.get_by_idempotency_key("missing") is None


async def test_mark_processed_updates_pending_payment(uow_factory: UowFactory) -> None:
    payment = make_payment()
    await _save(uow_factory, payment)
    processed_at = NOW + timedelta(seconds=3)

    async with uow_factory() as uow:
        updated = await uow.payments.mark_processed(
            payment.id, PaymentStatus.SUCCEEDED, processed_at
        )
        await uow.commit()

    assert updated is True
    loaded = await _load(uow_factory, payment)
    assert loaded.status is PaymentStatus.SUCCEEDED
    assert loaded.processed_at == processed_at


@pytest.mark.parametrize("second_status", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_mark_processed_second_call_changes_nothing(
    uow_factory: UowFactory, second_status: PaymentStatus
) -> None:
    payment = make_payment()
    await _save(uow_factory, payment)
    first_at = NOW + timedelta(seconds=3)

    async with uow_factory() as uow:
        assert await uow.payments.mark_processed(payment.id, PaymentStatus.FAILED, first_at)
        await uow.commit()
    async with uow_factory() as uow:
        again = await uow.payments.mark_processed(
            payment.id, second_status, first_at + timedelta(minutes=1)
        )
        await uow.commit()

    assert again is False
    loaded = await _load(uow_factory, payment)
    assert loaded.status is PaymentStatus.FAILED
    assert loaded.processed_at == first_at


async def test_mark_processed_unknown_payment_returns_false(uow_factory: UowFactory) -> None:
    async with uow_factory() as uow:
        assert await uow.payments.mark_processed(uuid7(), PaymentStatus.SUCCEEDED, NOW) is False


async def test_mark_webhook_sent_only_once(uow_factory: UowFactory) -> None:
    payment = make_payment()
    await _save(uow_factory, payment)
    first_at = NOW + timedelta(seconds=5)

    async with uow_factory() as uow:
        assert await uow.payments.mark_webhook_sent(payment.id, first_at) is True
        await uow.commit()
    async with uow_factory() as uow:
        assert await uow.payments.mark_webhook_sent(payment.id, first_at + timedelta(1)) is False
        await uow.commit()

    loaded = await _load(uow_factory, payment)
    assert loaded.webhook_sent_at == first_at


async def test_reread_in_same_uow_sees_conditional_update(uow_factory: UowFactory) -> None:
    # Reads must reflect the database, not an identity-map copy (reads use populate_existing).
    payment = make_payment()
    await _save(uow_factory, payment)

    async with uow_factory() as uow:
        before = await uow.payments.get_by_id(payment.id)
        assert before is not None and before.status is PaymentStatus.PENDING
        assert await uow.payments.mark_processed(payment.id, PaymentStatus.FAILED, NOW)
        after = await uow.payments.get_by_id(payment.id)
        assert after is not None
        assert after.status is PaymentStatus.FAILED
        assert after.processed_at == NOW


async def test_mark_processed_rejects_non_final_status(uow_factory: UowFactory) -> None:
    payment = make_payment()
    await _save(uow_factory, payment)
    async with uow_factory() as uow:
        with pytest.raises(InvalidStatusTransition, match="final status"):
            await uow.payments.mark_processed(payment.id, PaymentStatus.PENDING, NOW)


@pytest.mark.parametrize("first_commits", [True, False])
async def test_concurrent_insert_with_same_key(
    uow_factory: UowFactory, first_commits: bool
) -> None:
    # Two transactions race on one Idempotency-Key: the second INSERT waits on the unique
    # index; if the first commits it gets DO NOTHING and then sees the committed row,
    # if the first rolls back it inserts itself.
    first, second = make_payment("race"), make_payment("race")

    async def insert_second() -> tuple[bool, Payment | None]:
        async with uow_factory() as uow:
            inserted = await uow.payments.insert_if_absent(second)
            existing = await uow.payments.get_by_idempotency_key("race")
            await uow.commit()
            return inserted, existing

    async with uow_factory() as uow:
        assert await uow.payments.insert_if_absent(first)
        task = asyncio.create_task(insert_second())
        await asyncio.sleep(0.3)
        assert not task.done(), "second INSERT must wait for the first transaction"
        if first_commits:
            await uow.commit()
        else:
            await uow.rollback()

    inserted, existing = await asyncio.wait_for(task, timeout=5)
    assert existing is not None
    if first_commits:
        assert inserted is False
        assert existing.id == first.id
    else:
        assert inserted is True
        assert existing.id == second.id
