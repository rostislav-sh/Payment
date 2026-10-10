"""Payment gateway port: charges a payment and reports the business outcome."""

from typing import Protocol

from app.domain.enums import PaymentStatus
from app.domain.payment import Payment


class PaymentGateway(Protocol):
    async def process(self, payment: Payment) -> PaymentStatus:
        """Return SUCCEEDED or FAILED. A declined payment is a result, not an exception;
        raise GatewayUnavailable if the gateway itself could not be reached."""
        ...
