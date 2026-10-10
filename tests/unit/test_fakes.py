"""The fakes are reused by relay/consumer tests: pin the semantics they share with SQL."""

from dataclasses import replace
from uuid import uuid4

import pytest

from app.domain.enums import PaymentStatus
from app.domain.exceptions import InvalidStatusTransition
from app.domain.outbox import OutboxMessage
from app.domain.payment import Payment
from app.infrastructure.db.repositories import MAX_ERROR_LENGTH
from tests.unit.fakes import (
    FakeConcurrencyError,
    FakeIntegrityError,
    FakePendingRollbackError,
    FakeStore,
)
from tests.unit.support import CMD, NOW


def _payment() -> Payment:
    return Payment.new(CMD, "hash", NOW)


async def _seed_payment(store: FakeStore) -> Payment:
    payment = _payment()
    async with store.uow() as uow:
        assert await uow.payments.insert_if_absent(payment)
        await uow.commit()
    return payment


async def _seed_outbox(store: FakeStore, count: int) -> None:
    async with store.uow() as uow:
        for _ in range(count):
            await uow.outbox.add(OutboxMessage.payment_created(uuid4()))
        await uow.commit()


class TestTransactions:
    async def test_writes_are_invisible_to_others_until_commit(self, store: FakeStore) -> None:
        payment = _payment()
        async with store.uow() as writer:
            assert await writer.payments.insert_if_absent(payment)
            assert await writer.payments.get_by_id(payment.id) == payment
            async with store.uow() as reader:
                assert await reader.payments.get_by_id(payment.id) is None
            await writer.commit()
        assert store.payment_list() == [payment]

    async def test_leaving_without_commit_discards(self, store: FakeStore) -> None:
        async with store.uow() as uow:
            await uow.payments.insert_if_absent(_payment())
        assert store.payment_list() == []

    async def test_rollback_discards(self, store: FakeStore) -> None:
        payment = _payment()
        async with store.uow() as uow:
            await uow.payments.insert_if_absent(payment)
            await uow.rollback()
            assert await uow.payments.get_by_id(payment.id) is None

    async def test_integrity_error_requires_rollback(self, store: FakeStore) -> None:
        # Like an SQLAlchemy session after a failed flush: unusable until rollback().
        aggregate_id = uuid4()
        async with store.uow() as uow:
            await uow.outbox.add(OutboxMessage.payment_created(aggregate_id))
            with pytest.raises(FakeIntegrityError):
                await uow.outbox.add(OutboxMessage.payment_created(aggregate_id))
            with pytest.raises(FakePendingRollbackError):
                await uow.outbox.fetch_unpublished(limit=10)
            with pytest.raises(FakePendingRollbackError):
                await uow.commit()
            await uow.rollback()
            assert await uow.outbox.fetch_unpublished(limit=10) == []

    async def test_updates_of_different_columns_do_not_overwrite_each_other(
        self, store: FakeStore
    ) -> None:
        payment = await _seed_payment(store)
        async with store.uow() as a, store.uow() as b:
            assert await a.payments.mark_processed(payment.id, PaymentStatus.SUCCEEDED, NOW)
            assert await b.payments.mark_webhook_sent(payment.id, NOW)
            await a.commit()
            await b.commit()
        [stored] = store.payment_list()
        assert stored.status is PaymentStatus.SUCCEEDED
        assert stored.webhook_sent_at == NOW

    async def test_lost_race_on_guarded_update_fails_loudly(self, store: FakeStore) -> None:
        payment = await _seed_payment(store)
        async with store.uow() as a, store.uow() as b:
            assert await a.payments.mark_processed(payment.id, PaymentStatus.SUCCEEDED, NOW)
            assert await b.payments.mark_processed(payment.id, PaymentStatus.FAILED, NOW)
            await a.commit()
            with pytest.raises(FakeConcurrencyError):
                await b.commit()
        assert store.payment_list()[0].status is PaymentStatus.SUCCEEDED

    async def test_concurrent_failures_do_not_lose_increments(self, store: FakeStore) -> None:
        await _seed_outbox(store, 1)
        async with store.uow() as a, store.uow() as b:
            await a.outbox.register_failure(1, "a")
            await b.outbox.register_failure(1, "b")
            await a.commit()
            await b.commit()
        assert store.outbox_list()[0].attempts == 2


class TestPaymentRepository:
    async def test_insert_if_absent_is_keyed_by_idempotency_key(self, store: FakeStore) -> None:
        payment = _payment()
        async with store.uow() as uow:
            assert await uow.payments.insert_if_absent(payment)
            assert not await uow.payments.insert_if_absent(replace(payment, id=uuid4()))
            assert await uow.payments.get_by_idempotency_key(CMD.idempotency_key) == payment

    async def test_mark_processed_only_from_pending(self, store: FakeStore) -> None:
        payment = await _seed_payment(store)
        async with store.uow() as uow:
            with pytest.raises(InvalidStatusTransition):
                await uow.payments.mark_processed(payment.id, PaymentStatus.PENDING, NOW)
            assert await uow.payments.mark_processed(payment.id, PaymentStatus.FAILED, NOW)
            assert not await uow.payments.mark_processed(payment.id, PaymentStatus.SUCCEEDED, NOW)
            assert not await uow.payments.mark_processed(uuid4(), PaymentStatus.FAILED, NOW)

    async def test_mark_webhook_sent_only_once(self, store: FakeStore) -> None:
        payment = await _seed_payment(store)
        async with store.uow() as uow:
            assert await uow.payments.mark_webhook_sent(payment.id, NOW)
            assert not await uow.payments.mark_webhook_sent(payment.id, NOW)


class TestOutboxRepository:
    async def test_add_assigns_ids_and_created_at(self, store: FakeStore) -> None:
        await _seed_outbox(store, 2)
        assert [(m.id, m.created_at) for m in store.outbox_list()] == [(1, NOW), (2, NOW)]

    async def test_failed_insert_consumes_an_id(self, store: FakeStore) -> None:
        aggregate_id = uuid4()
        async with store.uow() as uow:
            await uow.outbox.add(OutboxMessage.payment_created(aggregate_id))
            with pytest.raises(FakeIntegrityError):
                await uow.outbox.add(OutboxMessage.payment_created(aggregate_id))
        await _seed_outbox(store, 1)
        assert [m.id for m in store.outbox_list()] == [3]

    async def test_fetch_unpublished_orders_limits_and_skips_published(
        self, store: FakeStore
    ) -> None:
        await _seed_outbox(store, 3)
        async with store.uow() as uow:
            assert [m.id for m in await uow.outbox.fetch_unpublished(limit=2)] == [1, 2]
            await uow.outbox.mark_published([1], NOW)
            assert [m.id for m in await uow.outbox.fetch_unpublished(limit=10)] == [2, 3]

    async def test_register_failure_truncates_error(self, store: FakeStore) -> None:
        await _seed_outbox(store, 1)
        async with store.uow() as uow:
            await uow.outbox.register_failure(1, "x" * (MAX_ERROR_LENGTH + 500))
            await uow.commit()
        [message] = store.outbox_list()
        assert message.attempts == 1
        assert message.last_error == "x" * MAX_ERROR_LENGTH
