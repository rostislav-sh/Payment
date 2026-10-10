"""Injectable time source, so services and tests agree on "now"."""

from collections.abc import Callable
from datetime import UTC, datetime

type Clock = Callable[[], datetime]  # returns an aware datetime


def utc_now() -> datetime:
    return datetime.now(UTC)
