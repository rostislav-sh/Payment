"""Event publisher port: delivers outbox events to the message broker."""

from typing import Protocol

from app.domain.outbox import OutboxMessage


class PublishFailed(Exception):
    """An outbox event was not confirmed by the broker; it stays unpublished."""


class EventRejected(PublishFailed):
    """The broker refused this particular message (unroutable or negatively confirmed).

    Other messages of the batch may still go through.
    """


class BrokerUnavailable(PublishFailed):
    """The broker cannot be reached (connection lost, timeout, blocked).

    The rest of the batch would fail the same way, so the relay stops the batch and backs off.
    """


class EventPublisher(Protocol):
    async def publish(self, message: OutboxMessage) -> None:
        """Return only after the broker confirmed the message; raise PublishFailed otherwise."""
        ...
