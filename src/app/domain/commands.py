from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from app.domain.enums import Currency


@dataclass(frozen=True, slots=True)
class CreatePaymentCommand:
    """Input of PaymentService.create; knows nothing about HTTP or Pydantic.

    metadata is a read-only Mapping and excluded from hashing (dicts are unhashable).
    """

    amount: Decimal
    currency: Currency
    description: str
    webhook_url: str
    idempotency_key: str
    metadata: Mapping[str, Any] = field(default_factory=dict, hash=False)
