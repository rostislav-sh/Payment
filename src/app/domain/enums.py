from enum import StrEnum
from typing import Final


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


FINAL_STATUSES: Final = frozenset({PaymentStatus.SUCCEEDED, PaymentStatus.FAILED})
