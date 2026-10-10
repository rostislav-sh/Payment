"""Use cases of the HTTP API: create a payment idempotently and read it back."""

import logging
from dataclasses import dataclass
from uuid import UUID

from app.application.clock import Clock, utc_now
from app.application.interfaces import UnitOfWorkFactory
from app.domain.commands import CreatePaymentCommand
from app.domain.exceptions import IdempotencyConflict, PaymentNotFound
from app.domain.hashing import compute_request_hash
from app.domain.outbox import OutboxMessage
from app.domain.payment import Payment

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class CreatePaymentResult:
    payment: Payment
    replayed: bool  # an existing payment returned for a repeated idempotency key


class PaymentService:
    def __init__(self, uow_factory: UnitOfWorkFactory, clock: Clock = utc_now) -> None:
        self._uow_factory = uow_factory
        self._clock = clock

    async def create(self, cmd: CreatePaymentCommand) -> CreatePaymentResult:
        request_hash = compute_request_hash(cmd)
        payment = Payment.new(cmd, request_hash, self._clock())

        async with self._uow_factory() as uow:
            if await uow.payments.insert_if_absent(payment):
                await uow.outbox.add(OutboxMessage.payment_created(payment.id))
                await uow.commit()
                logger.info("payment created payment_id=%s", payment.id)
                return CreatePaymentResult(payment, replayed=False)

            # Replay: nothing was written, so there is nothing to commit and no new event.
            # Under READ COMMITTED a concurrent INSERT with the same key waited on the unique
            # index and did nothing, so this SELECT sees the winner's committed row.
            existing = await uow.payments.get_by_idempotency_key(cmd.idempotency_key)

        if existing is None:
            # Payments are never deleted, so a conflicting key without a row is a bug.
            raise RuntimeError(
                f"insert conflicted on idempotency key {cmd.idempotency_key!r}, "
                "but no payment with this key was found"
            )
        if existing.request_hash != request_hash:
            # %r: the key comes from the client; repr keeps control characters out of the log.
            logger.warning("idempotency conflict key=%r", cmd.idempotency_key)
            raise IdempotencyConflict(cmd.idempotency_key)
        logger.info("idempotent replay payment_id=%s", existing.id)
        return CreatePaymentResult(existing, replayed=True)

    async def get(self, payment_id: UUID) -> Payment:
        async with self._uow_factory() as uow:
            payment = await uow.payments.get_by_id(payment_id)
        if payment is None:
            raise PaymentNotFound(payment_id)
        return payment
