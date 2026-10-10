"""Retry / DLQ decisions and headers (publishing replaced by a recorder)."""

from datetime import datetime
from typing import Any

import pytest
from faststream.rabbit import RabbitBroker, RabbitExchange

from app.application.errors import BrokerUnavailable, WebhookRejected, WebhookUnavailable
from app.infrastructure.broker import retry as retry_module
from app.infrastructure.broker.retry import (
    MAX_ERROR_ENTRIES,
    MAX_ERROR_ENTRY_LENGTH,
    Delivery,
    FailureRouter,
)
from app.infrastructure.broker.topology import DLX_EXCHANGE, MAX_RETRIES, RETRY_EXCHANGE

Published = dict[str, Any]


@pytest.fixture
def published(monkeypatch: pytest.MonkeyPatch) -> list[Published]:
    calls: list[Published] = []

    async def record(
        broker: RabbitBroker, body: Any, *, exchange: RabbitExchange, **kwargs: Any
    ) -> None:
        calls.append({"body": body, "exchange": exchange, **kwargs})

    monkeypatch.setattr(retry_module, "publish_confirmed", record)
    return calls


@pytest.fixture
def router() -> FailureRouter:
    return FailureRouter(RabbitBroker())  # never connected: publishing is replaced


def delivery(**headers: Any) -> Delivery:
    return Delivery(
        body=b'{"payment_id": "x"}',
        headers=headers,
        message_id="42",
        content_type="application/json",
    )


@pytest.mark.parametrize("retries_done", range(MAX_RETRIES))
async def test_transient_error_is_retried_with_next_delay(
    router: FailureRouter, published: list[Published], retries_done: int
) -> None:
    headers = {"x-attempt": retries_done} if retries_done else {}
    await router.route(delivery(**headers), WebhookUnavailable("503"))

    [call] = published
    assert call["exchange"] is RETRY_EXCHANGE
    assert call["routing_key"] == f"retry.{retries_done + 1}"
    assert call["headers"]["x-attempt"] == retries_done + 1
    assert call["headers"]["x-errors"][-1] == (
        f"attempt {retries_done + 1}: WebhookUnavailable: 503"
    )
    assert (call["body"], call["message_id"], call["content_type"]) == (
        b'{"payment_id": "x"}',
        "42",
        "application/json",
    )


async def test_retries_exhausted_goes_to_dlq_with_history(
    router: FailureRouter, published: list[Published]
) -> None:
    history = ["attempt 1: A: x", "attempt 2: A: x", "attempt 3: A: x"]
    await router.route(
        delivery(**{"x-attempt": MAX_RETRIES, "x-errors": history}), WebhookUnavailable("503")
    )

    [call] = published
    headers = call["headers"]
    assert call["exchange"] is DLX_EXCHANGE
    assert call["routing_key"] == ""
    assert headers["x-error-kind"] == "retries_exhausted"
    assert headers["x-attempts"] == MAX_RETRIES + 1
    assert headers["x-errors"] == [*history, "attempt 4: WebhookUnavailable: 503"]
    assert headers["x-original-routing-key"] == "payment.created"
    assert datetime.fromisoformat(headers["x-failed-at"]).utcoffset() is not None
    # A message re-published from the DLQ starts from scratch.
    assert "x-attempt" not in headers


async def test_permanent_error_goes_to_dlq_at_once(
    router: FailureRouter, published: list[Published]
) -> None:
    await router.route(delivery(), WebhookRejected("404"))

    [call] = published
    assert call["exchange"] is DLX_EXCHANGE
    assert call["headers"]["x-error-kind"] == "permanent"
    assert call["headers"]["x-attempts"] == 1


async def test_only_own_headers_are_copied(
    router: FailureRouter, published: list[Published]
) -> None:
    broker_headers = {"x-death": [{"count": 1}], "x-first-death-queue": "q", "x-delivery-count": 2}
    await router.route(delivery(**broker_headers), WebhookUnavailable("503"))
    assert set(published[0]["headers"]) == {"x-attempt", "x-errors"}


@pytest.mark.parametrize(
    "value",
    ["2", -1, True, None, 2.0, b"2"],
    ids=["str", "negative", "bool", "none", "float", "bytes"],
)
async def test_unusable_attempt_header_counts_as_first_delivery(
    router: FailureRouter, published: list[Published], value: Any
) -> None:
    await router.route(delivery(**{"x-attempt": value}), WebhookUnavailable("503"))
    assert published[0]["routing_key"] == "retry.1"


async def test_error_history_parsing_never_fails(
    router: FailureRouter, published: list[Published]
) -> None:
    junk = [b"\xff\xfe invalid utf-8", {"a": 1}, "y" * 1000, *[f"old {i}" for i in range(20)]]
    await router.route(delivery(**{"x-errors": junk}), WebhookUnavailable("503"))

    errors = published[0]["headers"]["x-errors"]
    assert len(errors) == MAX_ERROR_ENTRIES  # bounded: headers must fit into one frame
    assert errors[-1].startswith("attempt 1: WebhookUnavailable")
    assert all(len(entry) <= MAX_ERROR_ENTRY_LENGTH for entry in errors)


async def test_error_entry_is_one_short_line(
    router: FailureRouter, published: list[Published]
) -> None:
    await router.route(delivery(), WebhookRejected("line one\nline two " + "z" * 500))
    entry = published[0]["headers"]["x-errors"][0]
    assert "\n" not in entry
    assert entry.startswith("attempt 1: WebhookRejected: line one line two")
    assert len(entry) == MAX_ERROR_ENTRY_LENGTH


async def test_publish_failure_propagates(
    router: FailureRouter, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The caller must not ack: the original message is rejected/redelivered instead.
    async def fail(*args: Any, **kwargs: Any) -> None:
        raise BrokerUnavailable("down")

    monkeypatch.setattr(retry_module, "publish_confirmed", fail)
    with pytest.raises(BrokerUnavailable):
        await router.route(delivery(), WebhookUnavailable("503"))
