"""The one way this service publishes to RabbitMQ (relay events, consumer retries and DLQ).

Every message is persistent and mandatory, the call waits for the publisher confirm, and the
whole call is bounded by a timeout. Broker errors are translated into the application's
PublishFailed hierarchy so callers never handle aio-pika exceptions themselves.
"""

import asyncio
from typing import Any

from aio_pika.exceptions import AMQPError, DeliveryError
from aiormq.exceptions import ChannelInvalidStateError
from faststream.rabbit import RabbitBroker, RabbitExchange

from app.application.ports.publisher import BrokerUnavailable, EventRejected

PUBLISH_TIMEOUT_SECONDS = 5.0


async def publish_confirmed(
    broker: RabbitBroker,
    body: Any,
    *,
    exchange: RabbitExchange,
    routing_key: str,
    message_id: str | None = None,
    headers: dict[str, Any] | None = None,
) -> None:
    """Publish and return after the broker confirmed the message.

    Raises EventRejected for a returned (unroutable) or nacked message and BrokerUnavailable
    when the broker cannot be reached in time.
    """
    try:
        # asyncio.timeout bounds the whole call: FastStream's own ``timeout`` covers only the
        # write and the confirm, not waiting for a robust connection to come back.
        async with asyncio.timeout(PUBLISH_TIMEOUT_SECONDS):
            await broker.publish(
                body,  # dict -> JSON with content-type application/json; bytes as is
                exchange=exchange,
                routing_key=routing_key,
                persist=True,  # survives a broker restart
                mandatory=True,  # with on_return_raises: unroutable raises, never vanishes
                message_id=message_id,
                headers=headers,
                timeout=PUBLISH_TIMEOUT_SECONDS,
            )
    except DeliveryError as exc:  # basic.return (PublishError) or basic.nack
        raise EventRejected(f"broker refused message {message_id}") from exc
    except (TimeoutError, AMQPError, OSError, ChannelInvalidStateError) as exc:
        # OSError covers connection errors; ChannelInvalidStateError: the channel was closed
        # under us (connection lost, robust reconnect in progress).
        raise BrokerUnavailable(f"could not publish message {message_id}") from exc
