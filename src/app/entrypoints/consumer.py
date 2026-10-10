"""Payment consumer process: ``python -m app.entrypoints.consumer``.

Consumes ``payment.created`` from payments.new and runs ProcessPayment for each message.
Prefetch (CONSUMER_PREFETCH) bounds how many payments are processed concurrently.
FastStream handles SIGTERM/SIGINT: it stops consuming and waits for running handlers
(graceful_timeout) before closing the connection; the lifespan then closes our resources.

Error handling (until retry/DLQ routing is added): the subscriber uses REJECT_ON_ERROR, so a
message whose handler raises is rejected without requeue and dead-lettered to the DLQ. Never
nack with requeue: RabbitMQ 4.x does not count it against the delivery limit.
"""

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from typing import Any

import httpx
from faststream import AckPolicy, FastStream
from faststream.message import StreamMessage
from faststream.rabbit.annotations import RabbitMessage

from app.application.process_payment import ProcessPayment
from app.core.config import (
    get_broker_settings,
    get_consumer_settings,
    get_database_settings,
    get_logging_settings,
)
from app.core.logging import setup_logging
from app.infrastructure.broker.connection import make_broker
from app.infrastructure.broker.messages import PaymentCreatedEvent
from app.infrastructure.broker.topology import PAYMENTS_EXCHANGE, PAYMENTS_QUEUE, declare_topology
from app.infrastructure.db.engine import create_engine, create_sessionmaker
from app.infrastructure.db.uow import SqlUnitOfWork
from app.infrastructure.gateway import EmulatedGateway
from app.infrastructure.webhook import HttpWebhookSender

logger = logging.getLogger(__name__)


async def raw_body(message: StreamMessage[Any]) -> bytes:
    """Decoder that skips FastStream's JSON parsing: a malformed body must reach our handler
    (and our error handling) instead of failing inside the framework."""
    return message.body


def create_consumer_app() -> FastStream:
    # Settings are read here, not in the lifespan: a configuration error then fails fast
    # with pydantic's own message.
    log_level = get_logging_settings().log_level
    broker_settings = get_broker_settings()
    consumer_settings = get_consumer_settings()
    database_url = get_database_settings().database_url.get_secret_value()

    broker = make_broker(
        broker_settings.rabbitmq_url.get_secret_value(),
        prefetch=broker_settings.consumer_prefetch,
    )
    process: ProcessPayment | None = None  # set by the lifespan before consuming starts

    @contextlib.asynccontextmanager
    async def lifespan() -> AsyncIterator[None]:
        nonlocal process
        setup_logging(log_level)
        engine = create_engine(database_url)
        http = httpx.AsyncClient(timeout=consumer_settings.webhook_timeout, follow_redirects=False)
        try:
            # The whole topology (retry queues, DLQ) exists before the subscriber starts.
            await broker.connect()
            await declare_topology(broker)
            sessionmaker = create_sessionmaker(engine)
            process = ProcessPayment(
                uow_factory=lambda: SqlUnitOfWork(sessionmaker),
                gateway=EmulatedGateway(
                    consumer_settings.gateway_min_delay,
                    consumer_settings.gateway_max_delay,
                    consumer_settings.gateway_success_rate,
                ),
                webhook=HttpWebhookSender(http, timeout=consumer_settings.webhook_timeout),
            )
            yield  # FastStream starts the broker, consumes, and on shutdown stops it
        finally:
            # Also runs when startup fails half way (e.g. RabbitMQ is down).
            await http.aclose()
            await engine.dispose()

    app = FastStream(broker, lifespan=lifespan)

    @broker.subscriber(
        PAYMENTS_QUEUE,
        PAYMENTS_EXCHANGE,
        decoder=raw_body,
        ack_policy=AckPolicy.REJECT_ON_ERROR,
    )
    async def handle_payment_created(message: RabbitMessage) -> None:
        assert process is not None, "consuming starts only after the lifespan set it up"
        event = PaymentCreatedEvent.model_validate_json(message.body)
        result = await process.execute(event.payment_id)
        logger.info(
            "payment handled payment_id=%s status=%s processed_now=%s webhook_sent_now=%s "
            "message_id=%s",
            event.payment_id,
            result.status,
            result.processed_now,
            result.webhook_sent_now,
            message.message_id,
        )

    return app


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):  # Ctrl+C on Windows: no traceback
        asyncio.run(create_consumer_app().run())
