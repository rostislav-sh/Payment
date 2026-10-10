"""Consumer use case: process a created payment and notify the client.

Every step checks whether it was already done, because a message can be delivered more than
once (outbox at-least-once, consumer retries, redelivery after a crash):

1. read the payment;
2. if still pending: run the gateway, then store the result with a conditional UPDATE
   (WHERE status = 'pending'); if another delivery won, use the status from the database;
3. if final and the webhook was not sent yet: send it, then mark it sent.

So a retry after a failed webhook repeats only the webhook, never the processing. The gateway
call and the HTTP request run outside database transactions.
"""

import logging
from uuid import UUID

from app.application.clock import Clock, utc_now
from app.application.ports.gateway import PaymentGateway
from app.application.ports.uow import UnitOfWorkFactory
from app.application.ports.webhook import WebhookSender
from app.application.results import ProcessPaymentResult
from app.domain.exceptions import PaymentMissing
from app.domain.payment import Payment

logger = logging.getLogger(__name__)


class ProcessPayment:
    def __init__(
        self,
        uow_factory: UnitOfWorkFactory,
        gateway: PaymentGateway,
        webhook: WebhookSender,
        clock: Clock = utc_now,
    ) -> None:
        self._uow_factory = uow_factory
        self._gateway = gateway
        self._webhook = webhook
        self._clock = clock

    async def execute(self, payment_id: UUID) -> ProcessPaymentResult:
        payment = await self._load(payment_id)

        processed_now = False
        if not payment.is_final():
            outcome = await self._gateway.process(payment)
            async with self._uow_factory() as uow:
                processed_now = await uow.payments.mark_processed(
                    payment.id, outcome, self._clock()
                )
                await uow.commit()
            if processed_now:
                logger.info("payment processed payment_id=%s status=%s", payment.id, outcome)
            else:
                logger.info("payment already processed concurrently payment_id=%s", payment.id)
            # Re-read in both cases: the stored status and processed_at are what the webhook
            # reports (after a lost race the stored outcome differs from ours).
            payment = await self._load(payment_id)

        webhook_sent_now = False
        if payment.needs_webhook():
            await self._webhook.send(payment)
            async with self._uow_factory() as uow:
                webhook_sent_now = await uow.payments.mark_webhook_sent(payment.id, self._clock())
                await uow.commit()
            if webhook_sent_now:
                logger.info("webhook delivered payment_id=%s", payment.id)
            else:
                logger.info(
                    "webhook delivered, already marked concurrently payment_id=%s", payment.id
                )

        return ProcessPaymentResult(
            status=payment.status,
            processed_now=processed_now,
            webhook_sent_now=webhook_sent_now,
        )

    async def _load(self, payment_id: UUID) -> Payment:
        async with self._uow_factory() as uow:
            payment = await uow.payments.get_by_id(payment_id)
        if payment is None:
            raise PaymentMissing(payment_id)
        return payment
