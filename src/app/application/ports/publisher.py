"""Event publisher port: delivers outbox events to the message broker."""

from typing import Protocol

from app.domain.outbox import OutboxMessage


class EventPublisher(Protocol):
    async def publish(self, message: OutboxMessage) -> None:
        """Return only after the broker confirmed the message; raise PublishFailed otherwise."""
        ...
