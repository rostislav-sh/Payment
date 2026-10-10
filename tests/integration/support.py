"""Helpers shared by the integration tests: domain factories and in-loop Alembic runs."""

from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from alembic import command
from alembic.config import Config
from sqlalchemy import Connection

from app.domain.commands import CreatePaymentCommand
from app.domain.enums import Currency
from app.domain.hashing import compute_request_hash
from app.domain.payment import Payment
from app.infrastructure.db.uow import SqlUnitOfWork

UowFactory = Callable[[], SqlUnitOfWork]

ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"

# Microseconds on purpose: TIMESTAMPTZ must keep them through the round trip.
NOW = datetime(2026, 10, 10, 9, 0, 0, 123456, tzinfo=UTC)


def make_payment(
    idempotency_key: str = "key-1",
    *,
    amount: Decimal = Decimal("100"),
    currency: Currency = Currency.RUB,
    metadata: dict[str, Any] | None = None,
    now: datetime = NOW,
) -> Payment:
    cmd = CreatePaymentCommand(
        amount=amount,
        currency=currency,
        description="Оплата заказа #1",
        webhook_url="https://example.com/hook",
        idempotency_key=idempotency_key,
        metadata=metadata if metadata is not None else {"order_id": 1, "tags": ["a", "b"]},
    )
    return Payment.new(cmd, compute_request_hash(cmd), now)


def _alembic_config(connection: Connection) -> Config:
    cfg = Config(str(ALEMBIC_INI))
    cfg.attributes["connection"] = connection
    cfg.attributes["configure_logger"] = False
    return cfg


def upgrade_head(connection: Connection) -> None:
    command.upgrade(_alembic_config(connection), "head")


def downgrade_base(connection: Connection) -> None:
    command.downgrade(_alembic_config(connection), "base")
