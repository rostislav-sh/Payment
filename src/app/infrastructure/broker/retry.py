"""Where a failed payment message goes next: a delayed retry or the DLQ.

    transient error, retries_done < MAX_RETRIES -> payments.retry / retry.{retries_done+1}
                                                  (TTL 1/2/4 s, then back to payments.new)
    transient error, retries exhausted      -> payments.dlx, x-error-kind=retries_exhausted
    permanent error (or malformed message)  -> payments.dlx, x-error-kind=permanent

"3 attempts" from the assignment is read as the first delivery plus 3 retries (4 in total).

Our own ``x-attempt`` header counts logical attempts (each retry is a new publish); the broker's
``x-death`` is not used, its semantics changed between RabbitMQ versions. Only our headers are
copied: never ``x-death``/``x-first-death-*``/``x-delivery-count`` from the incoming message.

Note the two similar headers: ``x-attempt`` (retries already made, travels with retries) and
``x-attempts`` (total attempts, DLQ copies only).

Every publish goes through publish_confirmed (persist, mandatory, confirm, timeout) and the
caller acks the incoming message only afterwards: a crash in between yields a harmless
duplicate, never a lost message. If publishing fails, the exception propagates: the subscriber
rejects the message if the channel is alive (the queue dead-letters it as a safety net, without
our headers), otherwise it is redelivered when the channel closes (counted by the delivery
limit).
"""

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from faststream.rabbit import RabbitBroker

from app.domain.exceptions import TransientProcessingError
from app.infrastructure.broker.publishing import publish_confirmed
from app.infrastructure.broker.topology import (
    DLX_EXCHANGE,
    MAX_RETRIES,
    PAYMENT_CREATED_ROUTING_KEY,
    RETRY_EXCHANGE,
    retry_routing_key,
)

logger = logging.getLogger(__name__)

ATTEMPT_HEADER = "x-attempt"  # retries already made; absent on the first delivery
ERRORS_HEADER = "x-errors"  # history: "attempt N: ErrorType: message"
ATTEMPTS_HEADER = "x-attempts"  # DLQ only: how many attempts were made in total
ERROR_KIND_HEADER = "x-error-kind"  # DLQ only: FailureKind
FAILED_AT_HEADER = "x-failed-at"  # DLQ only: ISO 8601 UTC
ORIGINAL_ROUTING_KEY_HEADER = "x-original-routing-key"  # DLQ only: for a manual re-publish

MAX_ERROR_ENTRY_LENGTH = 300
# Keeps headers small even if a message re-published by hand carries a long history:
# the content header must fit into one AMQP frame.
MAX_ERROR_ENTRIES = 10


class FailureKind(StrEnum):
    PERMANENT = "permanent"
    RETRIES_EXHAUSTED = "retries_exhausted"


@dataclass(frozen=True, slots=True)
class Delivery:
    """The parts of an incoming message the router needs (independent of FastStream)."""

    body: bytes
    headers: Mapping[str, Any]
    message_id: str  # FastStream generates one if the publisher did not set it
    content_type: str | None = None

    @property
    def retries_done(self) -> int:
        value = self.headers.get(ATTEMPT_HEADER, 0)
        # Anything but a non-negative int (a bool, a string, a hand-edited re-publish) counts
        # as a fresh message. Parsing headers must never fail.
        return value if type(value) is int and value >= 0 else 0

    @property
    def error_history(self) -> list[str]:
        value = self.headers.get(ERRORS_HEADER)
        if not isinstance(value, list):
            return []
        entries = [_header_text(v) for v in value[-MAX_ERROR_ENTRIES:]]
        return [entry[:MAX_ERROR_ENTRY_LENGTH] for entry in entries]


class FailureRouter:
    def __init__(self, broker: RabbitBroker) -> None:
        self._broker = broker

    async def route(self, delivery: Delivery, error: Exception) -> None:
        """Publish the message to a retry queue or the DLQ; the caller acks afterwards."""
        retries_done = delivery.retries_done
        history = [*delivery.error_history, _error_entry(retries_done + 1, error)]
        errors = history[-MAX_ERROR_ENTRIES:]

        if isinstance(error, TransientProcessingError) and retries_done < MAX_RETRIES:
            await self._retry(delivery, retries_done + 1, errors)
            return
        kind = (
            FailureKind.RETRIES_EXHAUSTED
            if isinstance(error, TransientProcessingError)
            else FailureKind.PERMANENT
        )
        await self._dead_letter(delivery, kind, retries_done + 1, errors, error)

    async def _retry(self, delivery: Delivery, retry: int, errors: list[str]) -> None:
        await publish_confirmed(
            self._broker,
            delivery.body,
            exchange=RETRY_EXCHANGE,
            routing_key=retry_routing_key(retry),
            message_id=delivery.message_id,
            headers={ATTEMPT_HEADER: retry, ERRORS_HEADER: errors},
            content_type=delivery.content_type,
        )
        logger.warning(
            "message scheduled for retry message_id=%s retry=%d/%d error=%s",
            delivery.message_id,
            retry,
            MAX_RETRIES,
            errors[-1],
        )

    async def _dead_letter(
        self,
        delivery: Delivery,
        kind: FailureKind,
        attempts: int,
        errors: list[str],
        error: Exception,
    ) -> None:
        # No x-attempt here: a message re-published from the DLQ starts from scratch.
        await publish_confirmed(
            self._broker,
            delivery.body,
            exchange=DLX_EXCHANGE,
            routing_key="",  # fanout
            message_id=delivery.message_id,
            headers={
                ERRORS_HEADER: errors,
                ATTEMPTS_HEADER: attempts,
                ERROR_KIND_HEADER: kind.value,
                FAILED_AT_HEADER: datetime.now(UTC).isoformat(),
                ORIGINAL_ROUTING_KEY_HEADER: PAYMENT_CREATED_ROUTING_KEY,
            },
            content_type=delivery.content_type,
        )
        # The traceback (with chained causes, e.g. the DB error behind StorageUnavailable)
        # goes to the log only; headers carry just the type and a short message.
        logger.error(
            "message moved to DLQ message_id=%s kind=%s attempts=%d error=%s",
            delivery.message_id,
            kind,
            attempts,
            errors[-1],
            exc_info=error,
        )


def _header_text(value: Any) -> str:
    if isinstance(value, bytes):  # pamqp yields bytes only for invalid UTF-8
        return value.decode("utf-8", "replace")
    return value if isinstance(value, str) else repr(value)


def _error_entry(attempt: int, error: Exception) -> str:
    # Type and a short message only; tracebacks belong in the logs, not in headers.
    message = " ".join(str(error).split())  # one line
    return f"attempt {attempt}: {type(error).__name__}: {message}"[:MAX_ERROR_ENTRY_LENGTH]
