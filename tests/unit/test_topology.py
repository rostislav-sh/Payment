"""Static RabbitMQ topology: names, types, durability and arguments (no broker needed)."""

from itertools import pairwise

import pytest
from faststream.rabbit import ExchangeType, RabbitExchange

from app.domain.outbox import PAYMENT_CREATED
from app.infrastructure.broker.topology import (
    DELIVERY_LIMIT,
    DLQ,
    DLQ_NAME,
    DLX_EXCHANGE,
    DLX_EXCHANGE_NAME,
    MAX_RETRIES,
    PAYMENT_CREATED_ROUTING_KEY,
    PAYMENTS_EXCHANGE,
    PAYMENTS_EXCHANGE_NAME,
    PAYMENTS_QUEUE,
    PAYMENTS_QUEUE_NAME,
    RETRY_DELAYS_MS,
    RETRY_EXCHANGE,
    RETRY_EXCHANGE_NAME,
    RETRY_QUEUES,
    InvalidRetryAttempt,
    retry_queue_name,
    retry_routing_key,
)


def test_objects_reference_each_other_consistently() -> None:
    # The name constants and the objects must not drift apart: arguments refer to names.
    assert PAYMENTS_EXCHANGE.name == PAYMENTS_EXCHANGE_NAME
    assert RETRY_EXCHANGE.name == RETRY_EXCHANGE_NAME
    assert DLX_EXCHANGE.name == DLX_EXCHANGE_NAME
    assert PAYMENTS_QUEUE.name == PAYMENTS_QUEUE_NAME
    assert DLQ.name == DLQ_NAME
    assert PAYMENT_CREATED_ROUTING_KEY == PAYMENT_CREATED  # relay routes by event_type
    assert PAYMENTS_QUEUE.arguments["x-dead-letter-exchange"] == DLX_EXCHANGE.name
    assert PAYMENTS_QUEUE.arguments["x-delivery-limit"] == DELIVERY_LIMIT
    for queue in RETRY_QUEUES:
        assert queue.arguments["x-dead-letter-exchange"] == PAYMENTS_EXCHANGE.name
        assert queue.arguments["x-dead-letter-routing-key"] == PAYMENTS_QUEUE.routing_key


@pytest.mark.parametrize(
    ("exchange", "name", "exchange_type"),
    [
        (PAYMENTS_EXCHANGE, "payments", ExchangeType.DIRECT),
        (RETRY_EXCHANGE, "payments.retry", ExchangeType.DIRECT),
        (DLX_EXCHANGE, "payments.dlx", ExchangeType.FANOUT),
    ],
)
def test_exchanges(exchange: RabbitExchange, name: str, exchange_type: ExchangeType) -> None:
    assert exchange.name == name
    assert exchange.type == exchange_type
    assert exchange.durable is True
    assert exchange.auto_delete is False


def test_payments_queue_is_quorum_with_dlx_and_delivery_limit() -> None:
    assert PAYMENTS_QUEUE.name == "payments.new"
    assert PAYMENTS_QUEUE.durable is True
    assert PAYMENTS_QUEUE.exclusive is False
    assert PAYMENTS_QUEUE.auto_delete is False
    assert PAYMENTS_QUEUE.routing_key == "payment.created"
    # FastStream stores the queue type as the x-queue-type argument.
    assert PAYMENTS_QUEUE.arguments == {
        "x-queue-type": "quorum",
        "x-dead-letter-exchange": "payments.dlx",
        "x-delivery-limit": 5,
    }


def test_retry_delays_are_exponential() -> None:
    assert RETRY_DELAYS_MS == (1000, 2000, 4000)
    assert len(RETRY_DELAYS_MS) == MAX_RETRIES
    for previous, current in pairwise(RETRY_DELAYS_MS):
        assert current == previous * 2


@pytest.mark.parametrize("attempt", range(1, MAX_RETRIES + 1))
def test_retry_names(attempt: int) -> None:
    assert retry_queue_name(attempt) == f"payments.new.retry.{attempt}"
    assert retry_routing_key(attempt) == f"retry.{attempt}"


@pytest.mark.parametrize("attempt", [0, MAX_RETRIES + 1, -1])
def test_retry_names_reject_out_of_range(attempt: int) -> None:
    with pytest.raises(InvalidRetryAttempt) as exc_info:
        retry_queue_name(attempt)
    assert exc_info.value.attempt == attempt
    with pytest.raises(InvalidRetryAttempt):
        retry_routing_key(attempt)


def test_retry_queues_dead_letter_back_to_payments_after_ttl() -> None:
    assert len(RETRY_QUEUES) == MAX_RETRIES
    for attempt, queue in enumerate(RETRY_QUEUES, start=1):
        assert queue.name == f"payments.new.retry.{attempt}"
        assert queue.routing_key == f"retry.{attempt}"
        assert queue.durable is True
        assert queue.auto_delete is False
        assert queue.arguments == {
            "x-queue-type": "classic",
            "x-message-ttl": RETRY_DELAYS_MS[attempt - 1],
            "x-dead-letter-exchange": "payments",
            "x-dead-letter-routing-key": "payment.created",
        }


def test_dlq_is_classic_durable_without_dead_lettering() -> None:
    assert DLQ.name == "payments.new.dlq"
    assert DLQ.durable is True
    assert DLQ.auto_delete is False
    assert DLQ.arguments == {"x-queue-type": "classic"}
