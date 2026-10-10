"""RabbitMQ topology: the single source of truth for exchanges, queues and bindings.

    payments (direct) --payment.created--> payments.new (quorum) --consumer-->
    payments.retry (direct) --retry.N--> payments.new.retry.N (TTL, no consumers)
        --expired, dead-lettered to payments/payment.created--> payments.new
    payments.dlx (fanout) --> payments.new.dlq (no consumers, inspected manually)

Messages reach payments.dlx either explicitly (the consumer publishes a failure with its
error history) or as a safety net: rejected by payments.new or over its delivery limit.

The relay (publisher) and the consumer (subscriber) must use these exact objects: declaring a
queue with different arguments fails with PRECONDITION_FAILED (queue arguments are immutable).
"""

from faststream.rabbit import ExchangeType, QueueType, RabbitBroker, RabbitExchange, RabbitQueue

from app.domain.outbox import PAYMENT_CREATED

PAYMENTS_EXCHANGE_NAME = "payments"
RETRY_EXCHANGE_NAME = "payments.retry"
DLX_EXCHANGE_NAME = "payments.dlx"
PAYMENTS_QUEUE_NAME = "payments.new"
DLQ_NAME = "payments.new.dlq"
PAYMENT_CREATED_ROUTING_KEY = PAYMENT_CREATED

# Logical retries are counted by our own `x-attempt` header (set by the consumer on every
# retry publish, see broker/retry.py) rather than `x-death`, whose semantics changed.
MAX_RETRIES = 3
# Delay before retry attempt 1, 2, 3 (exponential backoff). TTLs live in code, not in a policy:
# simple to read and test; changing one requires re-creating the retry queue.
RETRY_DELAYS_MS = (1000, 2000, 4000)
# Safety net for poison messages that crash the consumer: after this many returns to the queue
# (a closed channel with unacked messages, or basic.reject with requeue) the quorum queue
# dead-letters the message instead of redelivering it forever. RabbitMQ 4.x does NOT count
# basic.nack(requeue=True), so the consumer must reject, never nack with requeue.
DELIVERY_LIMIT = 5

PAYMENTS_EXCHANGE = RabbitExchange(PAYMENTS_EXCHANGE_NAME, type=ExchangeType.DIRECT, durable=True)
RETRY_EXCHANGE = RabbitExchange(RETRY_EXCHANGE_NAME, type=ExchangeType.DIRECT, durable=True)
# Fanout: everything dead-lettered ends up in the single DLQ regardless of routing key.
DLX_EXCHANGE = RabbitExchange(DLX_EXCHANGE_NAME, type=ExchangeType.FANOUT, durable=True)

# Quorum: replicated, and supports x-delivery-limit. Rejected messages (unexpected errors)
# and messages over the delivery limit go to the DLX.
PAYMENTS_QUEUE = RabbitQueue(
    PAYMENTS_QUEUE_NAME,
    queue_type=QueueType.QUORUM,
    durable=True,
    routing_key=PAYMENT_CREATED_ROUTING_KEY,
    arguments={
        "x-dead-letter-exchange": DLX_EXCHANGE_NAME,
        "x-delivery-limit": DELIVERY_LIMIT,
    },
)


class InvalidRetryAttempt(ValueError):
    """A retry attempt outside 1..MAX_RETRIES: there is no retry queue for it.

    The consumer routes to the DLQ once MAX_RETRIES is reached, so this is a bug signal.
    """

    def __init__(self, attempt: int) -> None:
        super().__init__(f"retry attempt must be in 1..{MAX_RETRIES}, got {attempt}")
        self.attempt = attempt


def _check_attempt(attempt: int) -> None:
    if not 1 <= attempt <= MAX_RETRIES:
        raise InvalidRetryAttempt(attempt)


def retry_queue_name(attempt: int) -> str:
    _check_attempt(attempt)
    return f"{PAYMENTS_QUEUE_NAME}.retry.{attempt}"


def retry_routing_key(attempt: int) -> str:
    _check_attempt(attempt)
    return f"retry.{attempt}"


def _retry_queue(attempt: int) -> RabbitQueue:
    # One queue per delay level: TTL expiry only happens at the queue head, so mixing
    # different delays in one queue would make short delays wait behind long ones.
    # No consumers: an expired message is dead-lettered back to payments/payment.created.
    return RabbitQueue(
        retry_queue_name(attempt),
        durable=True,
        routing_key=retry_routing_key(attempt),
        arguments={
            "x-message-ttl": RETRY_DELAYS_MS[attempt - 1],
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE_NAME,
            "x-dead-letter-routing-key": PAYMENT_CREATED_ROUTING_KEY,
        },
    )


# Index is attempt - 1.
RETRY_QUEUES: tuple[RabbitQueue, ...] = tuple(
    _retry_queue(attempt) for attempt in range(1, MAX_RETRIES + 1)
)

DLQ = RabbitQueue(DLQ_NAME, durable=True)

_EXCHANGES = (PAYMENTS_EXCHANGE, RETRY_EXCHANGE, DLX_EXCHANGE)
# (queue, exchange it is bound to); the binding key is the queue's routing_key ("" for fanout).
_BINDINGS = (
    (PAYMENTS_QUEUE, PAYMENTS_EXCHANGE),
    *((queue, RETRY_EXCHANGE) for queue in RETRY_QUEUES),
    (DLQ, DLX_EXCHANGE),
)


async def declare_topology(broker: RabbitBroker) -> None:
    """Declare all exchanges, queues and bindings. The broker must be connected.

    Idempotent: safe to call from the relay and the consumer on every start, in any order.
    FastStream binds only the queues of its own subscribers, so the bindings are explicit here.
    """
    for queue, exchange in _BINDINGS:
        declared_exchange = await broker.declare_exchange(exchange)  # cached per broker
        declared_queue = await broker.declare_queue(queue)
        await declared_queue.bind(declared_exchange, routing_key=queue.routing_key)
