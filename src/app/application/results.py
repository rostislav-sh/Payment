"""Results returned by the application use cases."""

from dataclasses import dataclass

from app.domain.payment import Payment


@dataclass(frozen=True, slots=True)
class CreatePaymentResult:
    payment: Payment
    replayed: bool  # an existing payment returned for a repeated idempotency key
