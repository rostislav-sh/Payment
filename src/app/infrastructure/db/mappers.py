"""Conversion between domain entities and ORM models. ORM objects never leave this package."""

from typing import Any

from app.domain.enums import Currency, PaymentStatus
from app.domain.outbox import OutboxMessage
from app.domain.payment import Payment
from app.infrastructure.db.models import OutboxModel, PaymentModel


def payment_to_row(payment: Payment) -> dict[str, Any]:
    """Column values for a Core INSERT (keys are ORM attribute names)."""
    return {
        "id": payment.id,
        "amount": payment.amount,
        "currency": payment.currency.value,
        "description": payment.description,
        "meta": dict(payment.metadata),
        "status": payment.status.value,
        "idempotency_key": payment.idempotency_key,
        "request_hash": payment.request_hash,
        "webhook_url": payment.webhook_url,
        "created_at": payment.created_at,
        "processed_at": payment.processed_at,
        "webhook_sent_at": payment.webhook_sent_at,
    }


def payment_from_model(model: PaymentModel) -> Payment:
    return Payment(
        id=model.id,
        amount=model.amount,
        currency=Currency(model.currency),
        description=model.description,
        metadata=dict(model.meta),
        status=PaymentStatus(model.status),
        idempotency_key=model.idempotency_key,
        request_hash=model.request_hash,
        webhook_url=model.webhook_url,
        created_at=model.created_at,
        processed_at=model.processed_at,
        webhook_sent_at=model.webhook_sent_at,
    )


def outbox_to_model(message: OutboxMessage) -> OutboxModel:
    # id and created_at are always assigned by the database: an explicit id would not
    # advance the IDENTITY sequence and later inserts would collide.
    return OutboxModel(
        event_type=message.event_type,
        aggregate_id=message.aggregate_id,
        payload=dict(message.payload),
        published_at=message.published_at,
        attempts=message.attempts,
        last_error=message.last_error,
    )


def outbox_from_model(model: OutboxModel) -> OutboxMessage:
    return OutboxMessage(
        id=model.id,
        event_type=model.event_type,
        aggregate_id=model.aggregate_id,
        payload=dict(model.payload),
        created_at=model.created_at,
        published_at=model.published_at,
        attempts=model.attempts,
        last_error=model.last_error,
    )
