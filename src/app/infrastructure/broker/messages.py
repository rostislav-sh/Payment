"""Message bodies exchanged over RabbitMQ."""

from uuid import UUID

from pydantic import BaseModel, ConfigDict


class PaymentCreatedEvent(BaseModel):
    """Body of ``payment.created``: deliberately thin, the consumer reads the current state
    from the database. Unknown fields are ignored so the producer can add fields first."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    payment_id: UUID
