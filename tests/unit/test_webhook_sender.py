import asyncio
import json
import random
from decimal import Decimal

import httpx
import pytest

from app.application.errors import WebhookRejected, WebhookUnavailable
from app.domain.enums import PaymentStatus
from app.domain.payment import Payment
from app.infrastructure.gateway import EmulatedGateway
from app.infrastructure.webhook import HttpWebhookSender, build_payload
from tests.unit.support import CMD, NOW

URL = "https://client.example/hook"


def _payment() -> Payment:
    payment = Payment.new(CMD, "hash", NOW)
    payment.status = PaymentStatus.SUCCEEDED
    payment.processed_at = NOW
    return payment


def _sender(handler: httpx.MockTransport, timeout: float = 5) -> HttpWebhookSender:
    return HttpWebhookSender(httpx.AsyncClient(transport=handler), timeout=timeout)


def _answer(status: int) -> httpx.MockTransport:
    return httpx.MockTransport(lambda request: httpx.Response(status))


def _raise(error: Exception) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        raise error

    return httpx.MockTransport(handler)


async def test_payload_and_headers() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(204)

    payment = _payment()
    await _sender(httpx.MockTransport(handler)).send(payment)

    [request] = seen
    assert str(request.url) == payment.webhook_url
    assert request.headers["Idempotency-Key"] == str(payment.id)
    assert json.loads(request.content) == {
        "event": "payment.processed",
        "payment_id": str(payment.id),
        "status": "succeeded",
        "amount": "100.00",  # a string: no float rounding
        "currency": "RUB",
        "processed_at": NOW.isoformat(),
    }


@pytest.mark.parametrize("status", [200, 201, 204])
async def test_2xx_is_delivered(status: int) -> None:
    await _sender(_answer(status)).send(_payment())


@pytest.mark.parametrize("status", [500, 502, 503, 408, 429])
async def test_retryable_answers(status: int) -> None:
    with pytest.raises(WebhookUnavailable, match=str(status)):
        await _sender(_answer(status)).send(_payment())


@pytest.mark.parametrize("status", [400, 401, 404, 410, 301, 302])
async def test_permanent_answers(status: int) -> None:
    with pytest.raises(WebhookRejected, match=str(status)):
        await _sender(_answer(status)).send(_payment())


@pytest.mark.parametrize(
    "error",
    [
        httpx.ConnectError("refused"),
        httpx.ReadTimeout("slow"),
        httpx.PoolTimeout("pool"),
        httpx.RemoteProtocolError("broken"),
    ],
    ids=lambda e: type(e).__name__,
)
async def test_network_errors_are_retryable(error: Exception) -> None:
    with pytest.raises(WebhookUnavailable):
        await _sender(_raise(error)).send(_payment())


@pytest.mark.parametrize(
    "error",
    [httpx.UnsupportedProtocol("ftp"), httpx.InvalidURL("bad"), httpx.LocalProtocolError("x")],
    ids=lambda e: type(e).__name__,
)
async def test_unsendable_requests_are_permanent(error: Exception) -> None:
    with pytest.raises(WebhookRejected):
        await _sender(_raise(error)).send(_payment())


async def test_overall_timeout_bounds_a_slow_endpoint() -> None:
    async def slow(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(10)
        return httpx.Response(200)

    with pytest.raises(WebhookUnavailable):
        await asyncio.wait_for(
            _sender(httpx.MockTransport(slow), timeout=0.1).send(_payment()), timeout=2
        )


def test_payload_of_pending_payment_has_no_processed_at() -> None:
    payment = Payment.new(CMD, "hash", NOW)
    payload = build_payload(payment)
    assert payload["processed_at"] is None
    assert payload["amount"] == str(Decimal("100.00"))


class TestEmulatedGateway:
    async def test_outcomes_follow_success_rate(self) -> None:
        payment = _payment()
        always = EmulatedGateway(0, 0, success_rate=1.0, rng=random.Random(1))
        never = EmulatedGateway(0, 0, success_rate=0.0, rng=random.Random(1))
        assert await always.process(payment) is PaymentStatus.SUCCEEDED
        assert await never.process(payment) is PaymentStatus.FAILED

    async def test_roughly_ninety_percent_succeed(self) -> None:
        gateway = EmulatedGateway(0, 0, success_rate=0.9, rng=random.Random(42))
        outcomes = [await gateway.process(_payment()) for _ in range(1000)]
        share = outcomes.count(PaymentStatus.SUCCEEDED) / len(outcomes)
        assert 0.85 < share < 0.95
