from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from uuid import UUID

import pytest

from app.domain.commands import CreatePaymentCommand
from app.domain.enums import Currency, PaymentStatus
from app.domain.exceptions import InvalidPaymentData, NaiveDatetime
from app.domain.hashing import compute_request_hash
from app.domain.money import to_cents
from app.domain.outbox import PAYMENT_CREATED, OutboxMessage
from app.domain.payment import Payment

NOW = datetime(2026, 10, 10, 9, 0, tzinfo=UTC)

BASE_CMD = CreatePaymentCommand(
    amount=Decimal("100.00"),
    currency=Currency.RUB,
    description="Order #1",
    webhook_url="https://example.com/hook",
    idempotency_key="key-1",
    metadata={"order_id": 1, "tags": ["a", "b"]},
)

# sha256 of the canonical JSON
# {"amount":"100.00","currency":"RUB","description":"Order #1",
#  "metadata":{"order_id":1,"tags":["a","b"]},"webhook_url":"https://example.com/hook"}
# The hash is stored in the DB: if this test fails, idempotency breaks for existing keys.
GOLDEN_HASH = "14f3a7e6dffac47786ab45e8638661c47a371b9dbca3d4292f033db0fb4bdc57"
GOLDEN_HASH_CYRILLIC = "65797f53dac671ad4e5e0ec27d703d44c8ee608ce048953f9dc9365d7ab608dd"


class TestRequestHash:
    def test_golden_value(self) -> None:
        assert compute_request_hash(BASE_CMD) == GOLDEN_HASH

    def test_golden_value_non_ascii(self) -> None:
        # ensure_ascii=False: Cyrillic is hashed as UTF-8, not as \u escapes.
        cmd = replace(BASE_CMD, description="Оплата заказа")
        assert compute_request_hash(cmd) == GOLDEN_HASH_CYRILLIC

    @pytest.mark.parametrize("amount", ["100", "100.0", "100.000", "1E+2"])
    def test_amount_is_normalized(self, amount: str) -> None:
        assert compute_request_hash(replace(BASE_CMD, amount=Decimal(amount))) == GOLDEN_HASH

    def test_metadata_key_order_does_not_matter(self) -> None:
        a = replace(BASE_CMD, metadata={"x": 1, "y": {"b": 2, "a": 1}})
        b = replace(BASE_CMD, metadata={"y": {"a": 1, "b": 2}, "x": 1})
        assert compute_request_hash(a) == compute_request_hash(b)

    def test_idempotency_key_is_not_part_of_hash(self) -> None:
        assert compute_request_hash(replace(BASE_CMD, idempotency_key="other")) == GOLDEN_HASH

    @pytest.mark.parametrize(
        "changes",
        [
            {"amount": Decimal("100.01")},
            {"currency": Currency.USD},
            {"description": "Order #2"},
            {"metadata": {"order_id": 2, "tags": ["a", "b"]}},
            {"metadata": {"order_id": 1, "tags": ["b", "a"]}},  # list order is significant
            {"metadata": {"order_id": 1.0, "tags": ["a", "b"]}},  # strict: 1 != 1.0
            {"webhook_url": "https://example.com/other"},
        ],
    )
    def test_any_business_field_changes_hash(self, changes: dict[str, Any]) -> None:
        assert compute_request_hash(replace(BASE_CMD, **changes)) != GOLDEN_HASH

    @pytest.mark.parametrize(
        "metadata",
        [
            {"amount": Decimal("1")},
            {"at": NOW},
            {1: "non-str key", "a": 2},
            {"x": float("nan")},
        ],
    )
    def test_non_json_metadata_is_rejected(self, metadata: dict[object, object]) -> None:
        with pytest.raises(InvalidPaymentData):
            compute_request_hash(replace(BASE_CMD, metadata=metadata))  # type: ignore[arg-type]


class TestMoney:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [("5", "5.00"), ("0.01", "0.01"), ("100.000", "100.00"), ("1E+2", "100.00")],
    )
    def test_normalizes_to_cents(self, raw: str, expected: str) -> None:
        result = to_cents(Decimal(raw))
        assert result == Decimal(expected)
        assert str(result) == expected

    @pytest.mark.parametrize(
        ("raw", "match"),
        [
            ("10.001", "decimal places"),
            ("0.005", "decimal places"),
            ("NaN", "finite"),
            ("sNaN", "finite"),
            ("Infinity", "finite"),
            ("1E+30", "too large"),
            ("0", "positive"),
            ("-0", "positive"),
            ("-5", "positive"),
        ],
    )
    def test_rejects_invalid(self, raw: str, match: str) -> None:
        with pytest.raises(InvalidPaymentData, match=match):
            to_cents(Decimal(raw))


class TestPayment:
    def test_new_payment(self) -> None:
        cmd = replace(BASE_CMD, amount=Decimal("100"))
        p = Payment.new(cmd, request_hash=GOLDEN_HASH, now=NOW)

        assert isinstance(p.id, UUID)
        assert p.id.version == 7
        assert p.status is PaymentStatus.PENDING
        assert str(p.amount) == "100.00"
        assert p.currency is Currency.RUB
        assert p.description == cmd.description
        assert p.webhook_url == cmd.webhook_url
        assert p.idempotency_key == cmd.idempotency_key
        assert p.request_hash == GOLDEN_HASH
        assert p.created_at == NOW
        assert p.processed_at is None
        assert p.webhook_sent_at is None
        assert p.metadata == cmd.metadata

    def test_metadata_is_deep_copied(self) -> None:
        p = Payment.new(BASE_CMD, GOLDEN_HASH, NOW)
        assert p.metadata["tags"] is not BASE_CMD.metadata["tags"]
        p.metadata["tags"].append("c")
        assert BASE_CMD.metadata["tags"] == ["a", "b"]

    def test_ids_are_unique_and_time_ordered(self) -> None:
        a = Payment.new(BASE_CMD, GOLDEN_HASH, NOW)
        b = Payment.new(BASE_CMD, GOLDEN_HASH, NOW)
        assert a.id < b.id

    def test_rejects_naive_datetime(self) -> None:
        with pytest.raises(NaiveDatetime, match="timezone-aware"):
            Payment.new(BASE_CMD, GOLDEN_HASH, datetime(2026, 1, 1))

    def test_accepts_non_utc_aware_datetime(self) -> None:
        msk = datetime(2026, 10, 10, 12, 0, tzinfo=timezone(timedelta(hours=3)))
        assert Payment.new(BASE_CMD, GOLDEN_HASH, msk).created_at == NOW

    def test_rejects_invalid_amount(self) -> None:
        with pytest.raises(InvalidPaymentData, match="decimal places"):
            Payment.new(replace(BASE_CMD, amount=Decimal("1.001")), GOLDEN_HASH, NOW)

    @pytest.mark.parametrize(
        ("status", "sent_at", "is_final", "needs_webhook"),
        [
            (PaymentStatus.PENDING, None, False, False),
            (PaymentStatus.SUCCEEDED, None, True, True),
            (PaymentStatus.FAILED, None, True, True),
            (PaymentStatus.SUCCEEDED, NOW, True, False),
            (PaymentStatus.FAILED, NOW, True, False),
        ],
    )
    def test_predicates(
        self,
        status: PaymentStatus,
        sent_at: datetime | None,
        is_final: bool,
        needs_webhook: bool,
    ) -> None:
        p = Payment.new(BASE_CMD, GOLDEN_HASH, NOW)
        p.status = status
        p.webhook_sent_at = sent_at
        assert p.is_final() is is_final
        assert p.needs_webhook() is needs_webhook


class TestOutboxMessage:
    def test_payment_created_factory(self) -> None:
        p = Payment.new(BASE_CMD, GOLDEN_HASH, NOW)
        msg = OutboxMessage.payment_created(p.id)

        assert msg.event_type == PAYMENT_CREATED == "payment.created"
        assert msg.aggregate_id == p.id
        assert msg.payload == {"payment_id": str(p.id)}
        assert msg.id is None
        assert msg.published_at is None
        assert msg.attempts == 0


def test_enum_values_match_db_constraints() -> None:
    # The same literals are used in CHECK constraints (step 2) and the API contract.
    assert [c.value for c in Currency] == ["RUB", "USD", "EUR"]
    assert [s.value for s in PaymentStatus] == ["pending", "succeeded", "failed"]
