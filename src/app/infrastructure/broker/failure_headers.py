"""Headers of retried and dead-lettered payment messages, and parsing of incoming ones.

Our own ``x-attempt`` header counts logical attempts (each retry is a new publish); the broker's
``x-death`` is not used, its semantics changed between RabbitMQ versions. Only our headers are
copied: never ``x-death``/``x-first-death-*``/``x-delivery-count`` from the incoming message.

Note the two similar headers: ``x-attempt`` (retries already made, travels with retries) and
``x-attempts`` (total attempts, DLQ copies only).
"""

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

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
        entries = [header_text(v) for v in value[-MAX_ERROR_ENTRIES:]]
        return [entry[:MAX_ERROR_ENTRY_LENGTH] for entry in entries]


def header_text(value: Any) -> str:
    if isinstance(value, bytes):  # pamqp yields bytes only for invalid UTF-8
        return value.decode("utf-8", "replace")
    return value if isinstance(value, str) else repr(value)


def error_entry(attempt: int, error: Exception) -> str:
    # Type and a short message only; tracebacks belong in the logs, not in headers.
    message = " ".join(str(error).split())  # one line
    return f"attempt {attempt}: {type(error).__name__}: {message}"[:MAX_ERROR_ENTRY_LENGTH]
