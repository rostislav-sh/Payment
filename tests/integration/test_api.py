"""The HTTP API end to end against the real database (the session's migrated Postgres)."""

from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.api.app import create_app
from app.domain.outbox import PAYMENT_CREATED
from tests.integration.support import count_rows

pytestmark = pytest.mark.integration

API_KEY = "integration-key"
PAYMENTS = "/api/v1/payments"
BODY: dict[str, Any] = {
    "amount": "100.50",
    "currency": "EUR",
    "description": "Оплата заказа #1",
    "metadata": {"order_id": 1, "tags": ["a", "b"]},
    "webhook_url": "https://example.com/hook",
}
HEADERS = {"X-API-Key": API_KEY, "Idempotency-Key": "key-1"}


@pytest_asyncio.fixture(loop_scope="session")
async def client(engine: AsyncEngine) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(api_key=API_KEY, engine=engine)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        yield client


async def test_create_then_get_round_trip(client: httpx.AsyncClient, engine: AsyncEngine) -> None:
    created = await client.post(PAYMENTS, json=BODY, headers=HEADERS)
    assert created.status_code == 202, created.text
    payment_id = created.json()["payment_id"]

    response = await client.get(f"{PAYMENTS}/{payment_id}", headers={"X-API-Key": API_KEY})

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["payment_id"] == payment_id
    assert payload["status"] == "pending"
    assert payload["created_at"] == created.json()["created_at"]
    assert payload["processed_at"] is None
    assert {k: payload[k] for k in BODY} == BODY
    assert payload["idempotency_key"] == "key-1"

    assert await count_rows(engine) == (1, 1)
    async with engine.connect() as conn:
        row = (await conn.execute(text("SELECT event_type, aggregate_id FROM outbox"))).one()
    assert (row.event_type, str(row.aggregate_id)) == (PAYMENT_CREATED, payment_id)


async def test_replay_and_conflict(client: httpx.AsyncClient, engine: AsyncEngine) -> None:
    first = await client.post(PAYMENTS, json=BODY, headers=HEADERS)
    assert first.status_code == 202, first.text
    assert "Idempotent-Replayed" not in first.headers

    replay = await client.post(PAYMENTS, json={**BODY, "amount": 100.5}, headers=HEADERS)
    assert replay.status_code == 202, replay.text
    assert replay.headers["Idempotent-Replayed"] == "true"
    assert replay.json() == first.json()

    conflict = await client.post(PAYMENTS, json={**BODY, "amount": "1.00"}, headers=HEADERS)
    assert conflict.status_code == 409, conflict.text
    assert conflict.json()["error"]["code"] == "idempotency_conflict"

    assert await count_rows(engine) == (1, 1)


async def test_ready_checks_database(client: httpx.AsyncClient) -> None:
    response = await client.get("/ready")
    assert response.status_code == 200
    assert response.json() == {"status": "ready"}


async def test_upper_bounds_fit_the_database(client: httpx.AsyncClient) -> None:
    # The largest amount NUMERIC(18, 2) holds and the longest key VARCHAR(255) holds.
    body = {**BODY, "amount": "9999999999999999.99"}
    created = await client.post(
        PAYMENTS, json=body, headers={"X-API-Key": API_KEY, "Idempotency-Key": "k" * 255}
    )
    assert created.status_code == 202, created.text

    response = await client.get(
        f"{PAYMENTS}/{created.json()['payment_id']}", headers={"X-API-Key": API_KEY}
    )
    assert response.json()["amount"] == "9999999999999999.99"
    assert response.json()["idempotency_key"] == "k" * 255
