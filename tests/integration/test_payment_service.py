import asyncio
from dataclasses import replace
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.application.payment_service import CreatePaymentResult, PaymentService
from app.domain.exceptions import IdempotencyConflict, PaymentNotFound
from app.domain.hashing import compute_request_hash
from app.domain.outbox import PAYMENT_CREATED
from app.domain.payment import Payment
from tests.integration.support import (
    NOW,
    UowFactory,
    count_rows,
    make_command,
    wait_for_lock_wait,
)

pytestmark = pytest.mark.integration

CMD = make_command()


@pytest.fixture
def service(uow_factory: UowFactory) -> PaymentService:
    return PaymentService(uow_factory, clock=lambda: NOW)


async def test_create_persists_payment_and_event_then_replays(
    service: PaymentService, engine: AsyncEngine
) -> None:
    created = await service.create(CMD)
    assert created.replayed is False
    assert await count_rows(engine) == (1, 1)

    async with engine.connect() as conn:
        row = (
            await conn.execute(text("SELECT event_type, aggregate_id, payload FROM outbox"))
        ).one()
    assert tuple(row) == (
        PAYMENT_CREATED,
        created.payment.id,
        {"payment_id": str(created.payment.id)},
    )

    # Semantically equal body (100 vs 100.00) replays the stored row as read from the DB.
    replayed = await service.create(replace(CMD, amount=Decimal("100.00")))
    assert replayed.replayed is True
    assert replayed.payment == created.payment
    assert await service.get(created.payment.id) == created.payment

    with pytest.raises(IdempotencyConflict):
        await service.create(replace(CMD, amount=Decimal("200")))
    assert await count_rows(engine) == (1, 1)


async def test_get_unknown_raises_not_found(service: PaymentService) -> None:
    missing = uuid4()
    with pytest.raises(PaymentNotFound) as exc_info:
        await service.get(missing)
    assert exc_info.value.payment_id == missing


@pytest.mark.parametrize("first_commits", [True, False])
async def test_create_waits_for_concurrent_insert_with_same_key(
    service: PaymentService, uow_factory: UowFactory, engine: AsyncEngine, first_commits: bool
) -> None:
    # Deterministic race: the first transaction holds the key uncommitted; create() must
    # block on the unique index, then replay it (commit) or insert itself (rollback).
    first = Payment.new(CMD, compute_request_hash(CMD), NOW)

    async with uow_factory() as uow:
        assert await uow.payments.insert_if_absent(first)
        task = asyncio.create_task(service.create(CMD))
        await wait_for_lock_wait(engine)
        assert not task.done()
        if first_commits:
            await uow.commit()
        else:
            await uow.rollback()

    result = await asyncio.wait_for(task, timeout=5)
    if first_commits:
        assert result.replayed is True
        assert result.payment.id == first.id
        assert await count_rows(engine) == (1, 0)  # the first writer emitted no event here
    else:
        assert result.replayed is False
        assert result.payment.id != first.id
        assert await count_rows(engine) == (1, 1)


async def test_concurrent_same_key_different_bodies(
    service: PaymentService, engine: AsyncEngine
) -> None:
    # Smoke test of the public path; the deterministic race is the test above.
    outcomes = await asyncio.gather(
        service.create(CMD),
        service.create(replace(CMD, amount=Decimal("200"))),
        return_exceptions=True,
    )

    successes = [o for o in outcomes if isinstance(o, CreatePaymentResult)]
    conflicts = [o for o in outcomes if isinstance(o, IdempotencyConflict)]
    assert len(successes) == 1, outcomes
    assert len(conflicts) == 1, outcomes
    assert successes[0].replayed is False
    assert await count_rows(engine) == (1, 1)
