"""Errors raised by implementations of the application ports (app.application.ports).

They are part of the ports' contracts but live apart from the Protocols: services, the API and
the consumer catch them without importing any infrastructure. Consumer-side failures derive
from the domain's Permanent/TransientProcessingError, which decides between DLQ and retry.
"""

from app.domain.exceptions import PermanentProcessingError, TransientProcessingError

# --- storage (UnitOfWork) -------------------------------------------------------------------


class StorageUnavailable(TransientProcessingError):
    """The database could not be reached or the connection broke mid-transaction.

    Raised by the unit of work instead of driver errors; the transaction was rolled back.
    The consumer retries the message, the API answers 503.
    """


# --- broker (EventPublisher) ----------------------------------------------------------------


class PublishFailed(Exception):
    """A message was not confirmed by the broker; nothing is considered published."""


class EventRejected(PublishFailed):
    """The broker refused this particular message (unroutable or negatively confirmed).

    Other messages of a batch may still go through.
    """


class BrokerUnavailable(PublishFailed):
    """The broker cannot be reached (connection lost, timeout, blocked).

    Further publishes would fail the same way: the relay stops the batch and backs off.
    """


# --- payment gateway (PaymentGateway) -------------------------------------------------------


class GatewayUnavailable(TransientProcessingError):
    """The gateway could not be reached; the payment stays pending and is retried."""


# --- client webhook (WebhookSender) ---------------------------------------------------------


class WebhookRejected(PermanentProcessingError):
    """The client answered with an error a retry will not fix (4xx except 408/429, 3xx),
    or the request cannot be sent at all. The message goes to the DLQ."""


class WebhookUnavailable(TransientProcessingError):
    """The client could not be reached or is temporarily failing (network error, timeout,
    5xx, 408, 429). The message is retried with backoff."""
