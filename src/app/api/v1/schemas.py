"""Request/response models of the v1 API and their mapping to/from the domain."""

import json
from datetime import datetime
from decimal import Decimal
from typing import Any, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

from app.domain.commands import CreatePaymentCommand
from app.domain.enums import Currency, PaymentStatus
from app.domain.payment import Payment

METADATA_MAX_DEPTH = 32
METADATA_MAX_BYTES = 16 * 1024


def _plain_json(value: Any) -> Any:
    """Bodies are parsed with Decimal floats (see app.api.routing); metadata is free-form
    JSON, so its numbers go back to float as with a regular JSON parser."""
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {k: _plain_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_plain_json(v) for v in value]
    return value


def _check_metadata_shape(value: Any) -> None:
    """Depth limit (iterative: no recursion on hostile input) and no NUL characters,
    which PostgreSQL rejects in JSONB."""
    stack: list[tuple[Any, int]] = [(value, 1)]
    while stack:
        node, depth = stack.pop()
        if depth > METADATA_MAX_DEPTH:
            raise ValueError(f"metadata is nested deeper than {METADATA_MAX_DEPTH} levels")
        if isinstance(node, dict):
            for key, item in node.items():
                if "\x00" in key:
                    raise ValueError("metadata must not contain NUL characters")
                stack.append((item, depth + 1))
        elif isinstance(node, list):
            stack.extend((item, depth + 1) for item in node)
        elif isinstance(node, str) and "\x00" in node:
            raise ValueError("metadata must not contain NUL characters")


class CreatePaymentRequest(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "amount": "100.50",
                    "currency": "RUB",
                    "description": "Order #42",
                    "metadata": {"order_id": 42},
                    "webhook_url": "https://example.com/webhooks/payments",
                }
            ]
        },
    )

    # Number or string; at most 2 decimal places and 18 digits (fits NUMERIC(18, 2)).
    # Numbers are exact: bodies are parsed with Decimal floats (app.api.routing).
    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: Currency
    description: str = Field(min_length=1, max_length=1000)
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description=f"Free-form JSON object (max depth {METADATA_MAX_DEPTH}, "
        f"max {METADATA_MAX_BYTES // 1024} KiB).",
    )
    webhook_url: HttpUrl  # http/https only, max length 2083 (built into HttpUrl)

    @field_validator("description")
    @classmethod
    def _no_nul_in_description(cls, value: str) -> str:
        if "\x00" in value:  # PostgreSQL TEXT cannot store NUL
            raise ValueError("description must not contain NUL characters")
        return value

    @field_validator("metadata")
    @classmethod
    def _plain_bounded_metadata(cls, value: dict[str, Any]) -> dict[str, Any]:
        _check_metadata_shape(value)
        plain: dict[str, Any] = _plain_json(value)
        size = len(json.dumps(plain, ensure_ascii=False, default=str).encode())
        if size > METADATA_MAX_BYTES:
            raise ValueError(f"metadata is larger than {METADATA_MAX_BYTES} bytes")
        return plain

    @field_validator("webhook_url")
    @classmethod
    def _no_credentials_in_url(cls, value: HttpUrl) -> HttpUrl:
        # Credentials in the URL would be stored and returned by GET.
        if value.username or value.password:
            raise ValueError("webhook_url must not contain credentials")
        return value

    def to_command(self, idempotency_key: str) -> CreatePaymentCommand:
        return CreatePaymentCommand(
            amount=self.amount,
            currency=self.currency,
            description=self.description,
            metadata=self.metadata,
            # Normalized form (e.g. trailing "/" added to a bare host): part of the request hash.
            webhook_url=str(self.webhook_url),
            idempotency_key=idempotency_key,
        )


class PaymentCreatedResponse(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime

    @classmethod
    def from_domain(cls, payment: Payment) -> Self:
        return cls(payment_id=payment.id, status=payment.status, created_at=payment.created_at)


class PaymentResponse(BaseModel):
    payment_id: UUID
    # Serialized as a JSON string ("100.50") so no precision is lost.
    amount: Decimal = Field(examples=["100.50"])
    currency: Currency
    description: str
    metadata: dict[str, Any]
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None

    @classmethod
    def from_domain(cls, payment: Payment) -> Self:
        return cls(
            payment_id=payment.id,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=payment.metadata,
            status=payment.status,
            idempotency_key=payment.idempotency_key,
            webhook_url=payment.webhook_url,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
        )
