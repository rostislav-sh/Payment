"""Relay and retry routing against real Postgres and RabbitMQ (testcontainers)."""

import time
from collections.abc import AsyncIterator
from typing import Any
from uuid import uuid4

import pytest
import pytest_asyncio
from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.application.errors import EventRejected, StorageUnavailable, WebhookUnavailable
from app.application.outbox_relay import OutboxRelay
from app.domain.outbox import OutboxMessage
from app.infrastructure.broker.connection import make_broker
from app.infrastructure.broker.failure_headers import Delivery
from app.infrastructure.broker.publisher import RabbitEventPublisher
from app.infrastructure.broker.retry import FailureRouter
from app.infrastructure.broker.topology import (
    DLQ,
    PAYMENTS_QUEUE,
    RETRY_QUEUES,
    declare_topology,
)
from app.infrastructure.db.engine import create_engine, create_sessionmaker
from app.infrastructure.db.uow import SqlUnitOfWork
from tests.integration.support import RabbitMQEndpoints, UowFactory, receive

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def broker(rabbitmq: RabbitMQEndpoints) -> AsyncIterator[RabbitBroker]:
    broker = make_broker(rabbitmq.amqp_url)
    await broker.connect()
    await declare_topology(broker)
    yield broker
    await broker.stop()


@pytest_asyncio.fixture(autouse=True, loop_scope="session")
async def _empty_queues(broker: RabbitBroker) -> None:
    for queue in (PAYMENTS_QUEUE, *RETRY_QUEUES, DLQ):
        await (await broker.declare_queue(queue)).purge()


async def _seed_outbox(uow_factory: UowFactory, *messages: OutboxMessage) -> None:
    async with uow_factory() as uow:
        for message in messages:
            await uow.outbox.add(message)
        await uow.commit()


async def test_relay_publishes_persistent_json_event(
    broker: RabbitBroker, uow_factory: UowFactory
) -> None:
    payment_id = uuid4()
    await _seed_outbox(uow_factory, OutboxMessage.payment_created(payment_id))
    relay = OutboxRelay(uow_factory, RabbitEventPublisher(broker), batch_size=10)

    result = await relay.run_once()

    assert (result.published, result.failed) == (1, 0)
    message = await receive(broker, PAYMENTS_QUEUE)
    assert message.body == f'{{"payment_id":"{payment_id}"}}'.encode()
    assert message.message_id == "1"  # the outbox id
    assert message.content_type == "application/json"
    assert message.delivery_mode == 2  # persistent
    async with uow_factory() as uow:
        assert await uow.outbox.fetch_unpublished(10) == []


async def test_unroutable_event_is_rejected_and_stays_unpublished(
    broker: RabbitBroker, uow_factory: UowFactory
) -> None:
    unroutable = OutboxMessage(event_type="nobody.listens", aggregate_id=uuid4(), payload={})
    await _seed_outbox(uow_factory, unroutable)
    publisher = RabbitEventPublisher(broker)

    async with uow_factory() as uow:
        [message] = await uow.outbox.fetch_unpublished(10)
    with pytest.raises(EventRejected):
        await publisher.publish(message)

    result = await OutboxRelay(uow_factory, publisher, batch_size=10).run_once()
    assert (result.published, result.failed) == (0, 1)
    async with uow_factory() as uow:
        [still_pending] = await uow.outbox.fetch_unpublished(10)
    assert still_pending.attempts == 1


async def test_retry_goes_through_the_ttl_queue_and_comes_back(broker: RabbitBroker) -> None:
    delivery = Delivery(b'{"payment_id":"x"}', {}, message_id="7", content_type="application/json")
    started = time.monotonic()

    await FailureRouter(broker).route(delivery, WebhookUnavailable("503"))

    message = await receive(broker, PAYMENTS_QUEUE, within=10)
    assert time.monotonic() - started >= 0.8  # waited in payments.new.retry.1 (TTL 1 s)
    assert message.headers["x-attempt"] == 1
    assert message.headers["x-errors"] == ["attempt 1: WebhookUnavailable: 503"]
    assert (message.message_id, message.delivery_mode) == ("7", 2)


async def test_exhausted_message_lands_in_dlq_with_headers(broker: RabbitBroker) -> None:
    delivery = Delivery(b"{}", {"x-attempt": 3}, message_id="9")

    await FailureRouter(broker).route(delivery, WebhookUnavailable("503"))

    message = await receive(broker, DLQ)
    headers: dict[str, Any] = dict(message.headers)
    assert headers["x-error-kind"] == "retries_exhausted"
    assert headers["x-attempts"] == 4
    assert "x-attempt" not in headers


async def test_unreachable_database_is_storage_unavailable() -> None:
    engine = create_engine("postgresql+asyncpg://u:p@127.0.0.1:1/db")
    sessionmaker: async_sessionmaker[AsyncSession] = create_sessionmaker(engine)
    try:
        with pytest.raises(StorageUnavailable):
            async with SqlUnitOfWork(sessionmaker) as uow:
                await uow.payments.get_by_id(uuid4())
    finally:
        await engine.dispose()
