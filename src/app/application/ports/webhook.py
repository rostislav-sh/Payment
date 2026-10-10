"""Webhook port: notifies the client about the final state of a payment."""

from typing import Protocol

from app.domain.exceptions import PermanentProcessingError, TransientProcessingError
from app.domain.payment import Payment


class WebhookRejected(PermanentProcessingError):
    """The client answered with an error a retry will not fix (4xx except 408/429, 3xx),
    or the URL cannot be used at all. The message goes to the DLQ."""


class WebhookUnavailable(TransientProcessingError):
    """The client could not be reached or is temporarily failing (network error, timeout,
    5xx, 408, 429). The message is retried with backoff."""


class WebhookSender(Protocol):
    async def send(self, payment: Payment) -> None:
        """Deliver the notification; return only on a 2xx answer.

        Raises WebhookRejected or WebhookUnavailable otherwise. Delivery is at-least-once:
        the client deduplicates by payment_id.
        """
        ...
