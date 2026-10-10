"""Constants shared by unit tests."""

from datetime import UTC, datetime
from decimal import Decimal

from app.domain.commands import CreatePaymentCommand
from app.domain.enums import Currency

NOW = datetime(2026, 10, 10, 9, 0, tzinfo=UTC)

CMD = CreatePaymentCommand(
    amount=Decimal("100"),
    currency=Currency.RUB,
    description="Order #1",
    webhook_url="https://example.com/hook",
    idempotency_key="key-1",
    metadata={"order_id": 1, "tags": ["a", "b"]},
)


def fixed_clock() -> datetime:
    return NOW
