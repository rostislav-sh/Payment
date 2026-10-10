"""Webhook port: notifies the client about the final state of a payment."""

from typing import Protocol

from app.domain.payment import Payment


class WebhookSender(Protocol):
    async def send(self, payment: Payment) -> None:
        """Deliver the notification; return only on a 2xx answer.

        Raises WebhookRejected or WebhookUnavailable otherwise. Delivery is at-least-once:
        the client deduplicates by payment_id.
        """
        ...
