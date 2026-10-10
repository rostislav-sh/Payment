"""The database itself guards the invariants: checked with raw SQL, bypassing the ORM."""

from decimal import Decimal
from typing import Any
from uuid import uuid7

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = pytest.mark.integration

EXPECTED_CONSTRAINTS = {
    "pk_payments",
    "pk_outbox",
    "uq_payments_idempotency_key",
    "uq_outbox_event_type_aggregate_id",
    "ck_payments_amount_positive",
    "ck_payments_currency_valid",
    "ck_payments_status_valid",
}

INSERT_PAYMENT = text(
    "INSERT INTO payments "
    "(id, amount, currency, description, status, idempotency_key, request_hash, webhook_url) "
    "VALUES (:id, :amount, :currency, 'd', :status, :key, :hash, 'https://example.com/hook')"
)


def _payment_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "id": uuid7(),
        "amount": Decimal("1.00"),
        "currency": "RUB",
        "status": "pending",
        "key": str(uuid7()),
        "hash": "0" * 64,
    }
    return row | overrides


async def test_valid_row_is_accepted_and_server_defaults_apply(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO payments "
                "(id, amount, currency, description, idempotency_key, request_hash, webhook_url) "
                "VALUES (:id, 10, 'EUR', 'd', 'k', :hash, 'https://example.com/hook')"
            ),
            {"id": uuid7(), "hash": "0" * 64},
        )
        row = (await conn.execute(text("SELECT status, metadata, created_at FROM payments"))).one()
    assert row.status == "pending"
    assert row.metadata == {}
    assert row.created_at is not None


@pytest.mark.parametrize(
    ("overrides", "constraint"),
    [
        ({"amount": Decimal("0")}, "ck_payments_amount_positive"),
        ({"amount": Decimal("-1.00")}, "ck_payments_amount_positive"),
        ({"currency": "GBP"}, "ck_payments_currency_valid"),
        ({"status": "refunded"}, "ck_payments_status_valid"),
    ],
)
async def test_check_constraints_reject_invalid_rows(
    engine: AsyncEngine, overrides: dict[str, Any], constraint: str
) -> None:
    with pytest.raises(IntegrityError, match=constraint):
        async with engine.begin() as conn:
            await conn.execute(INSERT_PAYMENT, _payment_row(**overrides))


async def test_idempotency_key_is_unique(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.execute(INSERT_PAYMENT, _payment_row(key="same"))
    with pytest.raises(IntegrityError, match="uq_payments_idempotency_key"):
        async with engine.begin() as conn:
            await conn.execute(INSERT_PAYMENT, _payment_row(key="same"))


async def test_constraint_names(engine: AsyncEngine) -> None:
    # contype 'n' (NOT NULL, stored in pg_constraint since Postgres 18) is ignored.
    async with engine.connect() as conn:
        names = (
            await conn.execute(
                text(
                    "SELECT conname FROM pg_constraint "
                    "WHERE conrelid IN ('payments'::regclass, 'outbox'::regclass) "
                    "AND contype IN ('p', 'u', 'c', 'f')"
                )
            )
        ).scalars()
        assert set(names) == EXPECTED_CONSTRAINTS


async def test_indexes(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    "SELECT tablename, indexname, indexdef FROM pg_indexes "
                    "WHERE tablename IN ('payments', 'outbox')"
                )
            )
        ).all()
    indexes = {r.indexname: (r.tablename, r.indexdef) for r in rows}

    # Only the indexes backing PK/UNIQUE plus the partial outbox index; nothing on status.
    assert set(indexes) == {
        "pk_payments",
        "uq_payments_idempotency_key",
        "pk_outbox",
        "uq_outbox_event_type_aggregate_id",
        "ix_outbox_unpublished",
    }
    table, indexdef = indexes["ix_outbox_unpublished"]
    assert table == "outbox"
    assert "(id)" in indexdef
    assert "WHERE (published_at IS NULL)" in indexdef
