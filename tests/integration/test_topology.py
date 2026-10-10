"""RabbitMQ topology against a real broker: declaration, routing, TTL retry path, DLX."""

import asyncio
import time
from collections.abc import AsyncIterator

import aio_pika
import pytest
import pytest_asyncio
from aio_pika import IncomingMessage
from aio_pika.exceptions import PublishError
from faststream.rabbit import RabbitBroker, RabbitQueue

from app.infrastructure.broker.connection import make_broker
from app.infrastructure.broker.topology import (
    DELIVERY_LIMIT,
    DLQ,
    DLX_EXCHANGE,
    PAYMENTS_EXCHANGE,
    PAYMENTS_QUEUE,
    RETRY_EXCHANGE,
    RETRY_QUEUES,
    declare_topology,
)
from tests.integration.support import RabbitMQEndpoints, get_management_json

pytestmark = pytest.mark.integration

ALL_QUEUES: tuple[RabbitQueue, ...] = (PAYMENTS_QUEUE, *RETRY_QUEUES, DLQ)


@pytest.fixture(autouse=True)
def _clean_tables() -> None:
    """Override the package-wide autouse TRUNCATE so RabbitMQ-only runs don't start Postgres.

    Tests that need the database do not belong in this module.
    """


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def broker(rabbitmq: RabbitMQEndpoints) -> AsyncIterator[RabbitBroker]:
    broker = make_broker(rabbitmq.amqp_url)
    await broker.connect()
    yield broker
    await broker.stop()


@pytest_asyncio.fixture(autouse=True, loop_scope="session")
async def _empty_queues(broker: RabbitBroker) -> None:
    await declare_topology(broker)
    for queue in ALL_QUEUES:
        declared = await broker.declare_queue(queue)
        await declared.purge()


async def receive(
    broker: RabbitBroker, queue: RabbitQueue, *, within: float = 5
) -> IncomingMessage:
    """Poll ``queue`` (basic.get, auto-ack) until a message arrives or ``within`` seconds pass."""
    declared = await broker.declare_queue(queue)
    async with asyncio.timeout(within):
        while True:
            message = await declared.get(no_ack=True, fail=False)
            if message is not None:
                return message
            await asyncio.sleep(0.05)


async def queue_size(broker: RabbitBroker, queue: RabbitQueue) -> int:
    declared = await broker.declare_queue(queue)
    # Re-declaring with the same arguments is a no-op that reports the message count.
    declare_ok = await declared.declare()
    return int(declare_ok.message_count or 0)


async def test_declare_topology_is_idempotent(rabbitmq: RabbitMQEndpoints) -> None:
    # A fresh connection really re-declares everything (FastStream caches declarations per
    # broker): identical arguments must not raise PRECONDITION_FAILED, e.g. when the relay and
    # the consumer start in any order. Exactly one binding per pair is checked below.
    async with make_broker(rabbitmq.amqp_url) as other:
        await declare_topology(other)


async def test_server_state_matches_the_code(rabbitmq: RabbitMQEndpoints) -> None:
    for queue in ALL_QUEUES:
        info = await get_management_json(rabbitmq, f"/api/queues/%2F/{queue.name}")
        assert info["durable"] is True
        assert info["arguments"] == queue.arguments
    assert (await get_management_json(rabbitmq, "/api/queues/%2F/payments.new"))["type"] == "quorum"

    for exchange in (PAYMENTS_EXCHANGE, RETRY_EXCHANGE, DLX_EXCHANGE):
        info = await get_management_json(rabbitmq, f"/api/exchanges/%2F/{exchange.name}")
        assert (info["type"], info["durable"]) == (exchange.type.value, True)

    bound = ((PAYMENTS_QUEUE, PAYMENTS_EXCHANGE), *((q, RETRY_EXCHANGE) for q in RETRY_QUEUES))
    for queue, exchange in (*bound, (DLQ, DLX_EXCHANGE)):
        bindings = await get_management_json(
            rabbitmq, f"/api/bindings/%2F/e/{exchange.name}/q/{queue.name}"
        )
        assert [b["routing_key"] for b in bindings] == [queue.routing_key]


async def test_payment_created_is_routed_to_payments_queue(broker: RabbitBroker) -> None:
    await broker.publish(
        b"hello",
        exchange=PAYMENTS_EXCHANGE,
        routing_key="payment.created",
        persist=True,
        mandatory=True,
    )

    message = await receive(broker, PAYMENTS_QUEUE)
    assert message.body == b"hello"
    for queue in (*RETRY_QUEUES, DLQ):
        assert await queue_size(broker, queue) == 0


async def test_retry_message_returns_to_payments_queue_after_ttl(broker: RabbitBroker) -> None:
    started = time.monotonic()
    await broker.publish(
        b"retry-me",
        exchange=RETRY_EXCHANGE,
        routing_key="retry.1",
        persist=True,
        mandatory=True,
    )

    message = await receive(broker, PAYMENTS_QUEUE, within=10)
    elapsed = time.monotonic() - started

    assert message.body == b"retry-me"
    # TTL is 1 s; generous upper bound for Docker Desktop on Windows.
    assert 0.8 <= elapsed <= 6, elapsed
    assert message.headers["x-first-death-queue"] == "payments.new.retry.1"
    assert message.headers["x-first-death-reason"] == "expired"


async def test_dead_lettered_message_lands_in_dlq(broker: RabbitBroker) -> None:
    await broker.publish(
        b"dead",
        exchange=DLX_EXCHANGE,
        routing_key="anything",  # fanout ignores the key
        persist=True,
        mandatory=True,
    )

    message = await receive(broker, DLQ)
    assert message.body == b"dead"


async def test_rejected_payment_message_goes_to_dlq(broker: RabbitBroker) -> None:
    await broker.publish(
        b"poison",
        exchange=PAYMENTS_EXCHANGE,
        routing_key="payment.created",
        persist=True,
        mandatory=True,
    )
    queue = await broker.declare_queue(PAYMENTS_QUEUE)
    # The publisher confirm above means the message is already queued; get() does not wait.
    message = await queue.get(no_ack=False, timeout=5)
    await message.reject(requeue=False)

    dead = await receive(broker, DLQ)
    assert dead.body == b"poison"
    assert dead.headers["x-first-death-queue"] == "payments.new"
    assert dead.headers["x-first-death-reason"] == "rejected"


async def test_unroutable_mandatory_publish_raises(broker: RabbitBroker) -> None:
    with pytest.raises(PublishError):  # NO_ROUTE, not a nack
        await broker.publish(
            b"lost",
            exchange=PAYMENTS_EXCHANGE,
            routing_key="nope",
            persist=True,
            mandatory=True,
        )


async def _get_unacked(queue: aio_pika.abc.AbstractQueue) -> aio_pika.abc.AbstractIncomingMessage:
    """Poll until a message is delivered (without ack); the requeue after a close is async."""
    async with asyncio.timeout(5):
        while True:
            message = await queue.get(no_ack=False, fail=False)
            if message is not None:
                return message
            await asyncio.sleep(0.05)


async def test_message_over_delivery_limit_goes_to_dlq(
    broker: RabbitBroker, rabbitmq: RabbitMQEndpoints
) -> None:
    # A consumer that crashes mid-processing (channel closed, message unacked) returns the
    # message to the queue; after DELIVERY_LIMIT returns the quorum queue dead-letters it.
    await broker.publish(
        b"crashes-consumer",
        exchange=PAYMENTS_EXCHANGE,
        routing_key="payment.created",
        persist=True,
        mandatory=True,
    )
    for _ in range(DELIVERY_LIMIT + 1):
        connection = await aio_pika.connect(rabbitmq.amqp_url)
        async with connection:
            channel = await connection.channel()
            queue = await channel.get_queue(PAYMENTS_QUEUE.name, ensure=False)
            message = await _get_unacked(queue)
            assert message.body == b"crashes-consumer"
        # Leaving the block closes the connection without an ack: one "crash".

    dead = await receive(broker, DLQ)
    assert dead.body == b"crashes-consumer"
    assert dead.headers["x-first-death-reason"] == "delivery_limit"
    assert await queue_size(broker, PAYMENTS_QUEUE) == 0
