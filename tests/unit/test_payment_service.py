from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from uuid import uuid4

import pytest

from app.application.clock import utc_now
from app.application.payment_service import PaymentService
from app.domain.commands import CreatePaymentCommand
from app.domain.enums import Currency, PaymentStatus
from app.domain.exceptions import IdempotencyConflict, InvalidPaymentData, PaymentNotFound
from app.domain.outbox import PAYMENT_CREATED
from app.domain.payment import Payment
from tests.unit.fakes import FakePaymentRepository, FakeStore
from tests.unit.support import CMD, NOW, fixed_clock


@pytest.fixture
def service(store: FakeStore) -> PaymentService:
    return PaymentService(store.uow_factory(), clock=fixed_clock)


async def _set_status(store: FakeStore, payment: Payment, status: PaymentStatus) -> None:
    async with store.uow() as uow:
        assert await uow.payments.mark_processed(payment.id, status, NOW + timedelta(minutes=1))
        await uow.commit()


class TestCreate:
    async def test_new_payment_is_pending_and_emits_one_event(
        self, service: PaymentService, store: FakeStore
    ) -> None:
        result = await service.create(CMD)

        assert result.replayed is False
        payment = result.payment
        assert payment.status is PaymentStatus.PENDING
        assert payment.created_at == NOW
        assert str(payment.amount) == "100.00"  # quantized, not just equal by value
        # Committed: visible in the shared store, not only inside the UoW.
        assert store.payment_list() == [payment]
        [event] = store.outbox_list()
        assert event.event_type == PAYMENT_CREATED
        assert event.aggregate_id == payment.id
        assert event.payload == {"payment_id": str(payment.id)}
        assert event.published_at is None

    async def test_repeat_with_same_body_replays(
        self, service: PaymentService, store: FakeStore
    ) -> None:
        first = await service.create(CMD)
        second = await service.create(CMD)

        assert second.replayed is True
        assert second.payment == first.payment
        assert len(store.payment_list()) == 1
        assert len(store.outbox_list()) == 1

    async def test_semantically_equal_body_replays(
        self, service: PaymentService, store: FakeStore
    ) -> None:
        first = await service.create(
            replace(CMD, amount=Decimal("100"), metadata={"a": 1, "b": {"x": 1, "y": 2}})
        )
        second = await service.create(
            replace(CMD, amount=Decimal("100.00"), metadata={"b": {"y": 2, "x": 1}, "a": 1})
        )

        assert second.replayed is True
        assert second.payment.id == first.payment.id
        assert len(store.outbox_list()) == 1

    @pytest.mark.parametrize(
        "other",
        [
            pytest.param(replace(CMD, amount=Decimal("100.01")), id="amount"),
            pytest.param(replace(CMD, currency=Currency.USD), id="currency"),
            pytest.param(replace(CMD, description="Order #2"), id="description"),
            pytest.param(replace(CMD, metadata={"order_id": 2}), id="metadata"),
            pytest.param(replace(CMD, webhook_url="https://example.com/other"), id="webhook_url"),
        ],
    )
    async def test_same_key_different_body_conflicts(
        self, service: PaymentService, store: FakeStore, other: CreatePaymentCommand
    ) -> None:
        first = await service.create(CMD)

        with pytest.raises(IdempotencyConflict) as exc_info:
            await service.create(other)

        assert exc_info.value.idempotency_key == CMD.idempotency_key
        assert store.payment_list() == [first.payment]
        assert len(store.outbox_list()) == 1

    async def test_replay_returns_current_state(
        self, service: PaymentService, store: FakeStore
    ) -> None:
        first = await service.create(CMD)
        await _set_status(store, first.payment, PaymentStatus.SUCCEEDED)

        second = await service.create(CMD)

        assert second.replayed is True
        assert second.payment.status is PaymentStatus.SUCCEEDED
        assert second.payment.processed_at == NOW + timedelta(minutes=1)
        assert len(store.outbox_list()) == 1

    async def test_different_keys_create_two_payments(
        self, service: PaymentService, store: FakeStore
    ) -> None:
        a = await service.create(CMD)
        b = await service.create(replace(CMD, idempotency_key="key-2"))

        assert not a.replayed
        assert not b.replayed
        assert a.payment.id != b.payment.id
        assert [e.aggregate_id for e in store.outbox_list()] == [a.payment.id, b.payment.id]

    async def test_invalid_amount_writes_nothing(
        self, service: PaymentService, store: FakeStore
    ) -> None:
        with pytest.raises(InvalidPaymentData):
            await service.create(replace(CMD, amount=Decimal("1.001")))

        assert store.payment_list() == []
        assert store.outbox_list() == []
        assert store.commits == 0  # no transaction was even opened for writing

    async def test_conflict_without_stored_row_is_a_bug(
        self, service: PaymentService, store: FakeStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Payments are never deleted; if the key conflicts but no row exists, fail loudly.
        async def conflict(self: FakePaymentRepository, payment: Payment) -> bool:
            return False

        async def nothing(self: FakePaymentRepository, key: str) -> Payment | None:
            return None

        monkeypatch.setattr(FakePaymentRepository, "insert_if_absent", conflict)
        monkeypatch.setattr(FakePaymentRepository, "get_by_idempotency_key", nothing)

        with pytest.raises(RuntimeError, match="no payment with this key"):
            await service.create(CMD)
        assert store.outbox_list() == []


class TestGet:
    async def test_returns_existing_payment(self, service: PaymentService) -> None:
        created = await service.create(CMD)
        assert await service.get(created.payment.id) == created.payment

    async def test_unknown_id_raises_not_found(self, service: PaymentService) -> None:
        missing = uuid4()
        with pytest.raises(PaymentNotFound) as exc_info:
            await service.get(missing)
        assert exc_info.value.payment_id == missing


def test_default_clock_is_aware_utc() -> None:
    assert utc_now().utcoffset() == timedelta(0)
