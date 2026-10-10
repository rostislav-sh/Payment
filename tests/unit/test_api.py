"""Black-box tests of the HTTP API with the in-memory store (no database).

The app gets an engine pointing at a closed port: it never connects unless /ready is called.
"""

import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI

from app.api.app import create_app
from app.api.dependencies import get_payment_service
from app.application.payment_service import PaymentService
from app.application.results import CreatePaymentResult
from app.domain.commands import CreatePaymentCommand
from app.infrastructure.db.engine import create_engine
from tests.unit.fakes import FakeStore
from tests.unit.support import NOW, fixed_clock

API_KEY = "test-key"
AUTH = {"X-API-Key": API_KEY}
UNREACHABLE_DB = "postgresql+asyncpg://u:p@127.0.0.1:1/db"
PAYMENTS = "/api/v1/payments"

BODY: dict[str, Any] = {
    "amount": "100.00",
    "currency": "RUB",
    "description": "Order #1",
    "metadata": {"order_id": 1, "tags": ["a", "b"]},
    "webhook_url": "https://example.com/hook",
}

PAYMENT_KEYS = {
    "payment_id",
    "amount",
    "currency",
    "description",
    "metadata",
    "status",
    "idempotency_key",
    "webhook_url",
    "created_at",
    "processed_at",
}


def nested(depth: int) -> dict[str, Any]:
    node: dict[str, Any] = {}
    for _ in range(depth - 1):
        node = {"n": node}
    return node


def body(**changes: Any) -> dict[str, Any]:
    return {**BODY, **changes}


def body_without(field: str) -> dict[str, Any]:
    return {k: v for k, v in BODY.items() if k != field}


def headers(idempotency_key: str | None = "key-1") -> dict[str, str]:
    if idempotency_key is None:
        return dict(AUTH)
    return {**AUTH, "Idempotency-Key": idempotency_key}


def assert_error(response: httpx.Response, status: int, code: str) -> dict[str, Any]:
    """Unified error body: {"error": {"code", "message"[, "details"]}}; returns ``error``."""
    assert response.status_code == status, response.text
    payload = response.json()
    assert set(payload) == {"error"}
    error: dict[str, Any] = payload["error"]
    assert error["code"] == code
    assert isinstance(error["message"], str) and error["message"]
    return error


class ExplodingService(PaymentService):
    async def create(self, cmd: CreatePaymentCommand) -> CreatePaymentResult:
        raise RuntimeError("secret")


@asynccontextmanager
async def running_app(store: FakeStore) -> AsyncIterator[FastAPI]:
    engine = create_engine(UNREACHABLE_DB)
    app = create_app(api_key=API_KEY, engine=engine)
    app.dependency_overrides[get_payment_service] = lambda: PaymentService(
        store.uow_factory(), clock=fixed_clock
    )
    try:
        async with app.router.lifespan_context(app):
            yield app
    finally:
        await engine.dispose()


def make_client(app: FastAPI, *, raise_app_exceptions: bool = True) -> httpx.AsyncClient:
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=raise_app_exceptions)
    return httpx.AsyncClient(transport=transport, base_url="http://test")


@pytest_asyncio.fixture
async def app(store: FakeStore) -> AsyncIterator[FastAPI]:
    async with running_app(store) as app:
        yield app


@pytest_asyncio.fixture
async def client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    async with make_client(app) as client:
        yield client


async def create(client: httpx.AsyncClient, key: str = "key-1", **changes: Any) -> httpx.Response:
    return await client.post(PAYMENTS, json=body(**changes), headers=headers(key))


# --- 1. authentication -----------------------------------------------------------------


@pytest.mark.parametrize("api_key", [None, "wrong-key"], ids=["no-key", "wrong-key"])
@pytest.mark.parametrize("method", ["POST", "GET"])
async def test_requests_without_valid_api_key_are_unauthorized(
    client: httpx.AsyncClient, store: FakeStore, method: str, api_key: str | None
) -> None:
    request_headers = {"Idempotency-Key": "key-1"}
    if api_key is not None:
        request_headers["X-API-Key"] = api_key
    url = PAYMENTS if method == "POST" else f"{PAYMENTS}/{uuid4()}"
    response = await client.request(
        method, url, json=BODY if method == "POST" else None, headers=request_headers
    )

    error = assert_error(response, 401, "unauthorized")
    assert "details" not in error
    assert response.headers["WWW-Authenticate"] == "APIKey"
    assert store.payment_list() == []


async def test_health_needs_no_api_key(client: httpx.AsyncClient) -> None:
    response = await client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


async def test_ready_returns_503_when_database_is_unreachable(client: httpx.AsyncClient) -> None:
    error = assert_error(await client.get("/ready"), 503, "not_ready")
    assert "details" not in error


# --- 2-4. create, replay, conflict -----------------------------------------------------


async def test_create_returns_202_with_pending_payment(
    client: httpx.AsyncClient, store: FakeStore
) -> None:
    response = await create(client)

    assert response.status_code == 202, response.text
    payload = response.json()
    assert set(payload) == {"payment_id", "status", "created_at"}
    assert payload["status"] == "pending"
    assert datetime.fromisoformat(payload["created_at"]) == NOW
    assert "Idempotent-Replayed" not in response.headers
    [payment] = store.payment_list()
    assert UUID(payload["payment_id"]) == payment.id
    assert payment.amount == Decimal("100.00")
    assert payment.idempotency_key == "key-1"
    assert len(store.outbox_list()) == 1


@pytest.mark.parametrize(
    "replay_amount",
    [
        pytest.param("100.00", id="identical-body"),
        pytest.param(100, id="number-100-vs-string-100.00"),
        pytest.param("100", id="string-100-vs-100.00"),
    ],
)
async def test_replay_returns_same_payment_with_replayed_header(
    client: httpx.AsyncClient, store: FakeStore, replay_amount: object
) -> None:
    first = await create(client)
    replay = await create(client, amount=replay_amount)

    assert replay.status_code == 202, replay.text
    assert replay.headers["Idempotent-Replayed"] == "true"
    assert replay.json() == first.json()
    assert len(store.payment_list()) == 1
    assert len(store.outbox_list()) == 1


async def test_same_key_with_different_body_is_conflict(
    client: httpx.AsyncClient, store: FakeStore
) -> None:
    await create(client)
    response = await create(client, amount="200.00")

    error = assert_error(response, 409, "idempotency_conflict")
    assert error["message"] == "Idempotency-Key already used with a different request body"
    assert "details" not in error
    assert len(store.payment_list()) == 1


# --- 5. request validation -------------------------------------------------------------


@pytest.mark.parametrize(
    ("idempotency_key", "request_body", "field"),
    [
        pytest.param(None, BODY, "header.Idempotency-Key", id="missing-idempotency-key"),
        pytest.param("", BODY, "header.Idempotency-Key", id="empty-idempotency-key"),
        pytest.param("k" * 256, BODY, "header.Idempotency-Key", id="idempotency-key-256-chars"),
        pytest.param("key-1", body(amount=0), "body.amount", id="amount-zero"),
        pytest.param("key-1", body(amount=-1), "body.amount", id="amount-negative"),
        pytest.param("key-1", body(amount="1.001"), "body.amount", id="amount-3-decimals"),
        pytest.param(
            "key-1", body(amount="12345678901234567.00"), "body.amount", id="amount-19-digits"
        ),
        pytest.param(
            # 17 digits in total fits max_digits=18, but not NUMERIC(18, 2): 16 before the point.
            "key-1",
            body(amount="12345678901234567"),
            "body.amount",
            id="amount-17-int-digits",
        ),
        pytest.param("key 1", BODY, "header.Idempotency-Key", id="idempotency-key-with-space"),
        pytest.param(
            "key-1", body(description="a\x00b"), "body.description", id="description-with-nul"
        ),
        pytest.param(
            "key-1", body(metadata={"k": "a\x00b"}), "body.metadata", id="metadata-nul-value"
        ),
        pytest.param("key-1", body(metadata={"a\x00": 1}), "body.metadata", id="metadata-nul-key"),
        pytest.param("key-1", body(metadata=nested(33)), "body.metadata", id="metadata-too-deep"),
        pytest.param(
            "key-1", body(metadata={"x": "y" * 17_000}), "body.metadata", id="metadata-too-large"
        ),
        pytest.param(
            "key-1",
            body(webhook_url="https://user:pw@example.com/hook"),
            "body.webhook_url",
            id="webhook-url-with-credentials",
        ),
        pytest.param("key-1", body(amount="abc"), "body.amount", id="amount-not-a-number"),
        pytest.param("key-1", body(currency="GBP"), "body.currency", id="currency-unsupported"),
        pytest.param("key-1", body(description=""), "body.description", id="description-empty"),
        pytest.param(
            "key-1", body_without("webhook_url"), "body.webhook_url", id="webhook-url-missing"
        ),
        pytest.param(
            "key-1", body(webhook_url="not-a-url"), "body.webhook_url", id="webhook-url-invalid"
        ),
        pytest.param("key-1", body(unknown="x"), "body.unknown", id="extra-field"),
        pytest.param("key-1", b'{"amount": "100.00",', "body", id="malformed-json"),
    ],
)
async def test_invalid_request_is_422_with_field_details(
    client: httpx.AsyncClient,
    store: FakeStore,
    idempotency_key: str | None,
    request_body: dict[str, Any] | bytes,
    field: str,
) -> None:
    request_headers = headers(idempotency_key)
    if isinstance(request_body, bytes):
        request_headers["Content-Type"] = "application/json"
        response = await client.post(PAYMENTS, content=request_body, headers=request_headers)
    else:
        response = await client.post(PAYMENTS, json=request_body, headers=request_headers)

    error = assert_error(response, 422, "validation_error")
    details = error["details"]
    assert details, error
    for detail in details:
        assert set(detail) == {"field", "message"}
        assert isinstance(detail["message"], str) and detail["message"]
    # A malformed body's loc ends with the error position ("body.19"), so match by prefix.
    fields = [d["field"] for d in details]
    assert any(f == field or f.startswith(f"{field}.") for f in fields), fields
    assert store.payment_list() == []


# --- 6-7. amount formats, NaN in metadata ----------------------------------------------


@pytest.mark.parametrize(
    "amount", [pytest.param(100.5, id="json-number"), pytest.param("100.50", id="json-string")]
)
async def test_amount_is_accepted_as_number_or_string(
    client: httpx.AsyncClient, amount: object
) -> None:
    created = await create(client, amount=amount)
    assert created.status_code == 202, created.text

    response = await client.get(f"{PAYMENTS}/{created.json()['payment_id']}", headers=AUTH)
    assert response.json()["amount"] == "100.50"


async def test_nan_in_metadata_is_rejected(client: httpx.AsyncClient, store: FakeStore) -> None:
    raw = json.dumps(body(metadata={"x": float("nan")}))  # allow_nan=True writes a bare NaN
    assert "NaN" in raw

    response = await client.post(
        PAYMENTS,
        content=raw.encode(),
        headers={**headers(), "Content-Type": "application/json"},
    )

    error = assert_error(response, 422, "validation_error")
    assert error["details"] and error["details"][0]["field"] == "body"
    assert store.payment_list() == []


async def test_body_that_is_not_utf8_is_422(client: httpx.AsyncClient, store: FakeStore) -> None:
    raw = json.dumps(body(description="Оплата"), ensure_ascii=False).encode("cp1251")
    response = await client.post(
        PAYMENTS, content=raw, headers={**headers(), "Content-Type": "application/json"}
    )
    assert_error(response, 422, "validation_error")
    assert store.payment_list() == []


async def test_amount_sent_as_json_number_keeps_every_cent(client: httpx.AsyncClient) -> None:
    # A float parser would turn this into 1234567890123456.8 (bodies use Decimal floats).
    raw = json.dumps(body()).replace('"amount": "100.00"', '"amount": 1234567890123456.78')
    created = await client.post(
        PAYMENTS, content=raw, headers={**headers(), "Content-Type": "application/json"}
    )
    assert created.status_code == 202, created.text

    response = await client.get(f"{PAYMENTS}/{created.json()['payment_id']}", headers=AUTH)
    assert response.json()["amount"] == "1234567890123456.78"


async def test_metadata_numbers_stay_json_numbers(client: httpx.AsyncClient) -> None:
    metadata = {"rate": 1.5, "count": 3, "deep": {"x": 0.1}, "list": [2.25]}
    created = await create(client, metadata=metadata)
    assert created.status_code == 202, created.text

    response = await client.get(f"{PAYMENTS}/{created.json()['payment_id']}", headers=AUTH)
    assert response.json()["metadata"] == metadata


async def test_metadata_at_depth_limit_is_accepted(client: httpx.AsyncClient) -> None:
    assert (await create(client, metadata=nested(32))).status_code == 202


async def test_idempotency_key_of_255_chars_is_accepted(client: httpx.AsyncClient) -> None:
    assert (await create(client, key="k" * 255)).status_code == 202


# --- authentication vs validation order ------------------------------------------------


async def test_auth_is_checked_before_body_fields(client: httpx.AsyncClient) -> None:
    response = await client.post(
        PAYMENTS, json=body(amount="abc"), headers={"Idempotency-Key": "k"}
    )
    assert_error(response, 401, "unauthorized")


async def test_malformed_json_is_rejected_before_auth(client: httpx.AsyncClient) -> None:
    # FastAPI parses the JSON before running dependencies (documented in the README).
    response = await client.post(
        PAYMENTS, content=b"{", headers={"Content-Type": "application/json"}
    )
    assert_error(response, 422, "validation_error")


async def test_unsupported_method_is_405_with_allow_header(client: httpx.AsyncClient) -> None:
    response = await client.delete(PAYMENTS, headers=AUTH)
    assert_error(response, 405, "method_not_allowed")
    assert response.headers["allow"] == "POST"


# --- 8-9. read -------------------------------------------------------------------------


async def test_get_returns_full_payment(client: httpx.AsyncClient) -> None:
    created = (await create(client)).json()

    response = await client.get(f"{PAYMENTS}/{created['payment_id']}", headers=AUTH)

    assert response.status_code == 200, response.text
    payload = response.json()
    assert set(payload) == PAYMENT_KEYS  # request_hash, webhook_sent_at are not exposed
    assert payload["payment_id"] == created["payment_id"]
    assert payload["amount"] == "100.00"
    assert payload["currency"] == "RUB"
    assert payload["description"] == BODY["description"]
    assert payload["metadata"] == BODY["metadata"]
    assert payload["status"] == "pending"
    assert payload["idempotency_key"] == "key-1"
    assert payload["webhook_url"] == BODY["webhook_url"]
    assert datetime.fromisoformat(payload["created_at"]) == NOW
    assert payload["processed_at"] is None


async def test_get_unknown_payment_is_404(client: httpx.AsyncClient) -> None:
    response = await client.get(f"{PAYMENTS}/{uuid4()}", headers=AUTH)
    error = assert_error(response, 404, "payment_not_found")
    assert "details" not in error


async def test_get_with_malformed_id_is_422(client: httpx.AsyncClient) -> None:
    response = await client.get(f"{PAYMENTS}/not-a-uuid", headers=AUTH)
    error = assert_error(response, 422, "validation_error")
    assert [d["field"] for d in error["details"]] == ["path.payment_id"]


# --- 10-12. unknown route, unhandled errors, OpenAPI -----------------------------------


async def test_unknown_route_is_404_with_unified_body(client: httpx.AsyncClient) -> None:
    error = assert_error(await client.get("/no-such-route", headers=AUTH), 404, "not_found")
    assert "details" not in error


async def test_unhandled_exception_is_500_without_leaking_details(
    app: FastAPI, store: FakeStore, caplog: pytest.LogCaptureFixture
) -> None:
    app.dependency_overrides[get_payment_service] = lambda: ExplodingService(store.uow_factory())

    with caplog.at_level(logging.ERROR):
        async with make_client(app, raise_app_exceptions=False) as client:
            response = await client.post(PAYMENTS, json=BODY, headers=headers())

    error = assert_error(response, 500, "internal_error")
    assert "details" not in error
    assert "secret" not in response.text
    assert any(
        r.exc_info is not None and isinstance(r.exc_info[1], RuntimeError) for r in caplog.records
    )


async def test_openapi_documents_api_key_security_and_paths(client: httpx.AsyncClient) -> None:
    response = await client.get("/openapi.json")

    assert response.status_code == 200
    schema = response.json()
    schemes = schema["components"]["securitySchemes"].values()
    assert {"type": "apiKey", "in": "header", "name": "X-API-Key"} in [
        {k: s[k] for k in ("type", "in", "name")} for s in schemes
    ]
    assert {PAYMENTS, f"{PAYMENTS}/{{payment_id}}"} <= set(schema["paths"])
    paths = schema["paths"]
    assert paths[PAYMENTS]["post"]["security"] == [{"ApiKeyAuth": []}]
    assert paths[f"{PAYMENTS}/{{payment_id}}"]["get"]["security"] == [{"ApiKeyAuth": []}]
    assert "security" not in paths["/health"]["get"]
    responses = paths[PAYMENTS]["post"]["responses"]
    assert {"202", "401", "409", "422", "500"} <= set(responses)
    validation_schema = responses["422"]["content"]["application/json"]["schema"]
    assert validation_schema["$ref"].endswith("/ValidationErrorResponse")
