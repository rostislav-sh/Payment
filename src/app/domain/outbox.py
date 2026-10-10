from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

PAYMENT_CREATED = "payment.created"


@dataclass(slots=True)
class OutboxMessage:
    """Event waiting to be published. ``id`` is assigned by the database (IDENTITY)."""

    event_type: str
    aggregate_id: UUID
    payload: dict[str, Any]
    id: int | None = None
    created_at: datetime | None = None
    published_at: datetime | None = None
    attempts: int = 0
    last_error: str | None = None

    @classmethod
    def payment_created(cls, payment_id: UUID) -> OutboxMessage:
        # Thin payload: the consumer reads the current state from the database.
        return cls(
            event_type=PAYMENT_CREATED,
            aggregate_id=payment_id,
            payload={"payment_id": str(payment_id)},
        )
