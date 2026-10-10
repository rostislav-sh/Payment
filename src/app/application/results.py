"""Results returned by the application use cases."""

from dataclasses import dataclass

from app.domain.enums import PaymentStatus
from app.domain.payment import Payment


@dataclass(frozen=True, slots=True)
class CreatePaymentResult:
    payment: Payment
    replayed: bool  # an existing payment returned for a repeated idempotency key


@dataclass(frozen=True, slots=True)
class RelayBatchResult:
    fetched: int  # unpublished events taken in this batch
    published: int
    failed: int  # failure registered: rejected by the broker or hit by an outage
    aborted: bool  # the batch was cut short (broker down, unexpected error): back off


@dataclass(frozen=True, slots=True)
class ProcessPaymentResult:
    status: PaymentStatus  # final status as stored in the database
    processed_now: bool  # this delivery ran the gateway (False: already processed before)
    webhook_sent_now: bool  # this delivery sent the webhook (False: it was sent before)
