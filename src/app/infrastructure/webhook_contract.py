"""What the client's webhook receives and how its answers are classified."""

from app.domain.payment import Payment

WEBHOOK_EVENT = "payment.processed"
# Client errors that a later attempt may fix: request timeout and rate limiting.
RETRYABLE_CLIENT_ERRORS = frozenset({408, 429})


def build_payload(payment: Payment) -> dict[str, str | None]:
    return {
        "event": WEBHOOK_EVENT,
        "payment_id": str(payment.id),
        "status": payment.status.value,
        "amount": str(payment.amount),  # a string, like in the API: no float rounding
        "currency": payment.currency.value,
        "processed_at": payment.processed_at.isoformat() if payment.processed_at else None,
    }
