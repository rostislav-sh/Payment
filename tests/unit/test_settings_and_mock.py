from collections.abc import AsyncIterator, Iterator

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from pydantic import ValidationError

from app.core.config import ApiSettings, ConsumerSettings, clear_settings_cache
from app.entrypoints.webhook_mock import create_app as create_mock


@pytest.fixture(autouse=True)
def _fresh_settings() -> Iterator[None]:
    clear_settings_cache()
    yield
    clear_settings_cache()


class TestApiSettings:
    def test_valid_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("API_KEY", "s3cr3t-Key_1")
        assert ApiSettings(_env_file=None).api_key.get_secret_value() == "s3cr3t-Key_1"

    @pytest.mark.parametrize("key", ["", "with space", "tab\there", "ключ"])
    def test_invalid_key_is_rejected_without_echoing_it(
        self, monkeypatch: pytest.MonkeyPatch, key: str
    ) -> None:
        monkeypatch.setenv("API_KEY", key)
        with pytest.raises(ValidationError) as exc_info:
            ApiSettings(_env_file=None)
        if key:
            assert key not in str(exc_info.value)

    def test_missing_key_is_required(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("API_KEY", raising=False)
        with pytest.raises(ValidationError):
            ApiSettings(_env_file=None)


def test_gateway_delays_must_be_ordered(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GATEWAY_MIN_DELAY", "5")
    monkeypatch.setenv("GATEWAY_MAX_DELAY", "2")
    with pytest.raises(ValidationError, match="GATEWAY_MIN_DELAY"):
        ConsumerSettings(_env_file=None)


class TestWebhookMock:
    @pytest_asyncio.fixture
    async def client(self, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[httpx.AsyncClient]:
        monkeypatch.setenv("WEBHOOK_MOCK_FAIL_RATE", "0")
        app: FastAPI = create_mock()
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://mock") as client:
            yield client

    async def test_records_and_flags_duplicates(self, client: httpx.AsyncClient) -> None:
        headers = {"Idempotency-Key": "p1"}
        for _ in range(2):
            assert (
                await client.post("/hook", json={"status": "ok"}, headers=headers)
            ).status_code == 200

        records = (await client.get("/received")).json()
        assert [(r["answered"], r["duplicate"]) for r in records] == [(200, True), (200, False)]

    async def test_forced_failure_does_not_count_as_delivered(
        self, client: httpx.AsyncClient
    ) -> None:
        headers = {"Idempotency-Key": "p1"}
        assert (await client.post("/hook?fail=500", json={}, headers=headers)).status_code == 500
        await client.post("/hook", json={}, headers=headers)
        assert [r["duplicate"] for r in (await client.get("/received")).json()] == [False, False]

    @pytest.mark.parametrize("fail", ["150", "600", "abc"])
    async def test_invalid_fail_code(self, client: httpx.AsyncClient, fail: str) -> None:
        assert (await client.post(f"/hook?fail={fail}", json={})).status_code == 422

    async def test_clear(self, client: httpx.AsyncClient) -> None:
        await client.post("/hook", json={})
        assert (await client.delete("/received")).status_code == 204
        assert (await client.get("/received")).json() == []

    async def test_fail_rate(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("WEBHOOK_MOCK_FAIL_RATE", "1")
        clear_settings_cache()
        transport = httpx.ASGITransport(app=create_mock())
        async with httpx.AsyncClient(transport=transport, base_url="http://mock") as client:
            assert (await client.post("/hook", json={})).status_code == 500
            # An explicit ?fail wins over the random failures.
            assert (await client.post("/hook?fail=404", json={})).status_code == 404
