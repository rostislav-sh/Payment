"""RabbitMQ implementation of the EventPublisher port (used by the outbox relay)."""

from faststream.rabbit import RabbitBroker

from app.domain.outbox import OutboxMessage
from app.infrastructure.broker.publishing import publish_confirmed
from app.infrastructure.broker.topology import PAYMENTS_EXCHANGE


class RabbitEventPublisher:
    def __init__(self, broker: RabbitBroker) -> None:
        self._broker = broker

    async def publish(self, message: OutboxMessage) -> None:
        """Publish to the ``payments`` exchange with the event type as the routing key."""
        await publish_confirmed(
            self._broker,
            message.payload,
            exchange=PAYMENTS_EXCHANGE,
            routing_key=message.event_type,
            # Outbox id: correlates relay and consumer logs; retries/DLQ keep it.
            message_id=str(message.id),
        )
