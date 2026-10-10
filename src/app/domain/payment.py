import copy
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any, Self

from app.domain.commands import CreatePaymentCommand
from app.domain.enums import FINAL_STATUSES, Currency, PaymentStatus
from app.domain.exceptions import NaiveDatetime
from app.domain.money import to_cents


@dataclass(slots=True)
class Payment:
    id: uuid.UUID
    amount: Decimal
    currency: Currency
    description: str
    status: PaymentStatus
    idempotency_key: str
    request_hash: str
    webhook_url: str
    created_at: datetime
    metadata: dict[str, Any] = field(default_factory=dict)
    processed_at: datetime | None = None
    webhook_sent_at: datetime | None = None

    @classmethod
    def new(cls, cmd: CreatePaymentCommand, request_hash: str, now: datetime) -> Self:
        if now.utcoffset() is None:
            raise NaiveDatetime("now must be timezone-aware")
        return cls(
            # uuid7 is time-ordered: friendlier to the B-tree primary key than uuid4.
            id=uuid.uuid7(),
            amount=to_cents(cmd.amount),
            currency=cmd.currency,
            description=cmd.description,
            metadata=copy.deepcopy(dict(cmd.metadata)),
            status=PaymentStatus.PENDING,
            idempotency_key=cmd.idempotency_key,
            request_hash=request_hash,
            webhook_url=cmd.webhook_url,
            created_at=now,
        )

    def is_final(self) -> bool:
        return self.status in FINAL_STATUSES

    def needs_webhook(self) -> bool:
        return self.is_final() and self.webhook_sent_at is None
