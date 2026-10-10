"""SQL implementations of the repository ports. They never commit: the UoW owns the transaction."""

from collections.abc import Sequence
from datetime import datetime
from typing import Any, cast
from uuid import UUID

from sqlalchemy import CursorResult, Select, Update, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.enums import FINAL_STATUSES, PaymentStatus
from app.domain.outbox import OutboxMessage
from app.domain.payment import Payment
from app.infrastructure.db.mappers import (
    outbox_from_model,
    outbox_to_model,
    payment_from_model,
    payment_to_row,
)
from app.infrastructure.db.models import OutboxModel, PaymentModel

MAX_ERROR_LENGTH = 1000


async def _execute_update(session: AsyncSession, stmt: Update) -> int:
    """Run a bulk UPDATE without touching the identity map; return the affected row count."""
    result = await session.execute(stmt.execution_options(synchronize_session=False))
    return cast(CursorResult[Any], result).rowcount


class SqlPaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert_if_absent(self, payment: Payment) -> bool:
        stmt = (
            pg_insert(PaymentModel)
            .values(**payment_to_row(payment))
            .on_conflict_do_nothing(index_elements=["idempotency_key"])
            .returning(PaymentModel.id)
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none() is not None

    async def get_by_id(self, payment_id: UUID) -> Payment | None:
        return await self._get_one(select(PaymentModel).where(PaymentModel.id == payment_id))

    async def get_by_idempotency_key(self, key: str) -> Payment | None:
        return await self._get_one(select(PaymentModel).where(PaymentModel.idempotency_key == key))

    async def _get_one(self, stmt: Select[tuple[PaymentModel]]) -> Payment | None:
        # populate_existing: always take column values from the database. Bulk UPDATEs below
        # skip the identity map, so a still-referenced ORM object could otherwise be stale.
        model = await self._session.scalar(stmt.execution_options(populate_existing=True))
        return payment_from_model(model) if model is not None else None

    async def mark_processed(
        self, payment_id: UUID, status: PaymentStatus, processed_at: datetime
    ) -> bool:
        if status not in FINAL_STATUSES:
            raise ValueError(f"mark_processed needs a final status, got {status}")
        stmt = (
            update(PaymentModel)
            .where(
                PaymentModel.id == payment_id,
                PaymentModel.status == PaymentStatus.PENDING.value,
            )
            .values(status=status.value, processed_at=processed_at)
        )
        return await _execute_update(self._session, stmt) == 1

    async def mark_webhook_sent(self, payment_id: UUID, sent_at: datetime) -> bool:
        stmt = (
            update(PaymentModel)
            .where(PaymentModel.id == payment_id, PaymentModel.webhook_sent_at.is_(None))
            .values(webhook_sent_at=sent_at)
        )
        return await _execute_update(self._session, stmt) == 1


class SqlOutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, message: OutboxMessage) -> None:
        self._session.add(outbox_to_model(message))
        # Flush now so a duplicate surfaces as IntegrityError at the call site. It is a bug,
        # not a recoverable case: the session is unusable afterwards, the whole UoW rolls back.
        await self._session.flush()

    async def fetch_unpublished(self, limit: int) -> list[OutboxMessage]:
        # The predicate matches ix_outbox_unpublished literally (published_at IS NULL).
        stmt = (
            select(OutboxModel)
            .where(OutboxModel.published_at.is_(None))
            .order_by(OutboxModel.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
            # Fresh values even if this session already holds the objects (bulk UPDATEs
            # in register_failure/mark_published bypass the identity map).
            .execution_options(populate_existing=True)
        )
        models = await self._session.scalars(stmt)
        return [outbox_from_model(m) for m in models]

    async def mark_published(self, ids: Sequence[int], published_at: datetime) -> None:
        if not ids:
            return
        stmt = update(OutboxModel).where(OutboxModel.id.in_(ids)).values(published_at=published_at)
        await _execute_update(self._session, stmt)

    async def register_failure(self, message_id: int, error: str) -> None:
        stmt = (
            update(OutboxModel)
            .where(OutboxModel.id == message_id)
            .values(attempts=OutboxModel.attempts + 1, last_error=error[:MAX_ERROR_LENGTH])
        )
        await _execute_update(self._session, stmt)
