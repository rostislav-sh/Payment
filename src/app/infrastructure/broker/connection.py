from faststream.rabbit import Channel, RabbitBroker


def make_broker(url: str, *, prefetch: int = 10) -> RabbitBroker:
    """Create a (not yet connected) broker shared by the relay and the consumer.

    Publisher confirms are on by default, so ``publish`` returns only after RabbitMQ has
    accepted the message; callers must pass ``timeout=`` or a blocked broker (memory/disk
    alarm) makes them wait forever. ``on_return_raises`` turns a returned (unroutable)
    mandatory publish into ``aio_pika.exceptions.PublishError`` instead of a silent loss.
    ``prefetch`` bounds how many unacked messages the consumer handles concurrently.
    """
    return RabbitBroker(
        url,
        default_channel=Channel(on_return_raises=True, prefetch_count=prefetch),
        graceful_timeout=15.0,
    )
