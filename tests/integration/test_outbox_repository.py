import asyncio
from datetime import timedelta
from uuid import uuid7

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from app.domain.outbox import PAYMENT_CREATED, OutboxMessage
from app.infrastructure.db.repositories import MAX_ERROR_LENGTH
from tests.integration.support import NOW, UowFactory, make_payment

pytestmark = pytest.mark.integration

# A fetch that waits on a row lock would hang forever; fail fast instead.
LOCK_TIMEOUT = 5.0


async def _seed(uow_factory: UowFactory, count: int) -> list[int]:
    """Insert ``count`` messages in one transaction; ids are 1..count after TRUNCATE."""
    async with uow_factory() as uow:
        for _ in range(count):
            await uow.outbox.add(OutboxMessage.payment_created(uuid7()))
        await uow.commit()
    return list(range(1, count + 1))


async def _fetch_ids(uow_factory: UowFactory, limit: int = 100) -> list[int | None]:
    async with uow_factory() as uow:
        return [m.id for m in await uow.outbox.fetch_unpublished(limit)]


async def test_add_then_fetch_unpublished(uow_factory: UowFactory) -> None:
    message = OutboxMessage.payment_created(uuid7())
    async with uow_factory() as uow:
        await uow.outbox.add(message)
        await uow.commit()

    async with uow_factory() as uow:
        fetched = await uow.outbox.fetch_unpublished(10)

    assert len(fetched) == 1
    got = fetched[0]
    assert isinstance(got.id, int)
    assert got.event_type == PAYMENT_CREATED
    assert got.aggregate_id == message.aggregate_id
    assert got.payload == {"payment_id": str(message.aggregate_id)}
    assert got.attempts == 0
    assert got.last_error is None
    assert got.published_at is None
    assert got.created_at is not None
    assert got.created_at.utcoffset() is not None


async def test_duplicate_event_rolls_back_whole_transaction(uow_factory: UowFactory) -> None:
    payment = make_payment()

    with pytest.raises(IntegrityError):
        async with uow_factory() as uow:
            await uow.payments.insert_if_absent(payment)
            await uow.outbox.add(OutboxMessage.payment_created(payment.id))
            await uow.outbox.add(OutboxMessage.payment_created(payment.id))
            await uow.commit()

    async with uow_factory() as uow:
        assert await uow.payments.get_by_id(payment.id) is None
        assert await uow.outbox.fetch_unpublished(10) == []


async def test_fetch_unpublished_orders_by_id_and_respects_limit(
    uow_factory: UowFactory,
) -> None:
    await _seed(uow_factory, 3)

    assert await _fetch_ids(uow_factory, limit=2) == [1, 2]
    assert await _fetch_ids(uow_factory) == [1, 2, 3]


async def test_mark_published_hides_rows(uow_factory: UowFactory, engine: AsyncEngine) -> None:
    await _seed(uow_factory, 3)
    published_at = NOW + timedelta(seconds=1)

    async with uow_factory() as uow:
        await uow.outbox.mark_published([1, 3], published_at)
        await uow.commit()

    assert await _fetch_ids(uow_factory) == [2]
    async with engine.connect() as conn:
        rows = (await conn.execute(text("SELECT id, published_at FROM outbox ORDER BY id"))).all()
    assert [(r.id, r.published_at) for r in rows] == [
        (1, published_at),
        (2, None),
        (3, published_at),
    ]


async def test_mark_published_with_empty_ids_is_noop(uow_factory: UowFactory) -> None:
    await _seed(uow_factory, 2)

    async with uow_factory() as uow:
        await uow.outbox.mark_published([], NOW)
        await uow.commit()

    assert await _fetch_ids(uow_factory) == [1, 2]


async def test_register_failure_increments_attempts_and_truncates_error(
    uow_factory: UowFactory,
) -> None:
    await _seed(uow_factory, 1)
    long_error = "x" * (MAX_ERROR_LENGTH + 500)

    async with uow_factory() as uow:
        await uow.outbox.register_failure(1, "first")
        await uow.outbox.register_failure(1, long_error)
        await uow.commit()

    async with uow_factory() as uow:
        [message] = await uow.outbox.fetch_unpublished(10)
    assert message.attempts == 2
    assert message.last_error == long_error[:MAX_ERROR_LENGTH]
    assert message.published_at is None  # a failure does not publish


async def test_fetch_unpublished_skips_rows_locked_by_another_uow(
    uow_factory: UowFactory,
) -> None:
    await _seed(uow_factory, 3)

    async with uow_factory() as uow_a:
        batch_a = await uow_a.outbox.fetch_unpublished(2)
        async with uow_factory() as uow_b:
            batch_b = await asyncio.wait_for(
                uow_b.outbox.fetch_unpublished(10), timeout=LOCK_TIMEOUT
            )

        assert [m.id for m in batch_a] == [1, 2]
        assert [m.id for m in batch_b] == [3]

    # Locks live only as long as the UoW: once A is gone, everything is visible again.
    assert await _fetch_ids(uow_factory) == [1, 2, 3]


async def test_refetch_in_same_uow_sees_failure_update(uow_factory: UowFactory) -> None:
    await _seed(uow_factory, 1)
    async with uow_factory() as uow:
        [message] = await uow.outbox.fetch_unpublished(limit=10)
        assert message.id is not None
        await uow.outbox.register_failure(message.id, "boom")
        [again] = await uow.outbox.fetch_unpublished(limit=10)
        assert again.attempts == 1
        assert again.last_error == "boom"
