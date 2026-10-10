"""Helpers shared by the integration tests: domain factories, in-loop Alembic runs, RabbitMQ."""

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import aio_pika
import httpx
from aio_pika import IncomingMessage
from alembic import command
from alembic.config import Config
from faststream.rabbit import RabbitBroker, RabbitQueue
from sqlalchemy import Connection, text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.domain.commands import CreatePaymentCommand
from app.domain.enums import Currency
from app.domain.hashing import compute_request_hash
from app.domain.payment import Payment
from app.infrastructure.db.uow import SqlUnitOfWork

UowFactory = Callable[[], SqlUnitOfWork]

ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"

# Microseconds on purpose: TIMESTAMPTZ must keep them through the round trip.
NOW = datetime(2026, 10, 10, 9, 0, 0, 123456, tzinfo=UTC)


def make_command(
    idempotency_key: str = "key-1",
    *,
    amount: Decimal = Decimal("100"),
    currency: Currency = Currency.RUB,
    metadata: dict[str, Any] | None = None,
) -> CreatePaymentCommand:
    return CreatePaymentCommand(
        amount=amount,
        currency=currency,
        description="Оплата заказа #1",
        webhook_url="https://example.com/hook",
        idempotency_key=idempotency_key,
        metadata=metadata if metadata is not None else {"order_id": 1, "tags": ["a", "b"]},
    )


def make_payment(
    idempotency_key: str = "key-1",
    *,
    amount: Decimal = Decimal("100"),
    currency: Currency = Currency.RUB,
    metadata: dict[str, Any] | None = None,
    now: datetime = NOW,
) -> Payment:
    cmd = make_command(idempotency_key, amount=amount, currency=currency, metadata=metadata)
    return Payment.new(cmd, compute_request_hash(cmd), now)


async def count_rows(engine: AsyncEngine) -> tuple[int, int]:
    """(payments, outbox) row counts."""
    async with engine.connect() as conn:
        payments = await conn.scalar(text("SELECT count(*) FROM payments"))
        outbox = await conn.scalar(text("SELECT count(*) FROM outbox"))
    return int(payments or 0), int(outbox or 0)


async def wait_for_lock_wait(engine: AsyncEngine) -> None:
    """Block (up to 5 s) until some backend waits on a lock, e.g. an INSERT on a unique index."""
    query = text(
        "SELECT count(*) FROM pg_stat_activity "
        "WHERE datname = current_database() AND wait_event_type = 'Lock'"
    )
    async with asyncio.timeout(5):
        while True:
            async with engine.connect() as conn:
                if await conn.scalar(query):
                    return
            await asyncio.sleep(0.02)


def _alembic_config(connection: Connection) -> Config:
    cfg = Config(str(ALEMBIC_INI))
    cfg.attributes["connection"] = connection
    cfg.attributes["configure_logger"] = False
    return cfg


def upgrade_head(connection: Connection) -> None:
    command.upgrade(_alembic_config(connection), "head")


def downgrade_base(connection: Connection) -> None:
    command.downgrade(_alembic_config(connection), "base")


# --- RabbitMQ --------------------------------------------------------------------------------

RABBITMQ_IMAGE = "rabbitmq:4.3-management"  # keep in sync with docker-compose.yml
RABBITMQ_USER = "guest"
RABBITMQ_PASSWORD = "guest"


@dataclass(frozen=True, slots=True)
class RabbitMQEndpoints:
    amqp_url: str
    management_url: str


async def wait_for_rabbitmq(endpoints: RabbitMQEndpoints, *, within: float = 90) -> None:
    """Poll until RabbitMQ accepts AMQP connections and the management API answers.

    Docker Desktop publishes the port before the broker listens (connections get reset),
    so a successful AMQP handshake is the only reliable readiness signal.
    """
    async with asyncio.timeout(within):
        while True:
            try:
                connection = await aio_pika.connect(endpoints.amqp_url, timeout=5)
            except Exception:  # any failure means "not ready yet"
                await asyncio.sleep(0.5)
                continue
            await connection.close()
            break
        async with httpx.AsyncClient(auth=(RABBITMQ_USER, RABBITMQ_PASSWORD)) as client:
            while True:
                try:
                    response = await client.get(f"{endpoints.management_url}/api/overview")
                    if response.status_code == 200:
                        return
                except httpx.HTTPError:
                    pass
                await asyncio.sleep(0.5)


async def get_management_json(endpoints: RabbitMQEndpoints, path: str) -> Any:
    """GET ``path`` (e.g. ``/api/queues/%2F/payments.new``) from the management HTTP API."""
    async with httpx.AsyncClient(auth=(RABBITMQ_USER, RABBITMQ_PASSWORD)) as client:
        response = await client.get(f"{endpoints.management_url}{path}")
    response.raise_for_status()
    return response.json()


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
