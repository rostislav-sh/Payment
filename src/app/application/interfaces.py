"""Ports of the application layer. Infrastructure implements them; services depend only on
these Protocols, so tests can substitute in-memory fakes."""

from collections.abc import Callable, Sequence
from datetime import datetime
from types import TracebackType
from typing import Protocol, Self
from uuid import UUID

from app.domain.enums import PaymentStatus
from app.domain.outbox import OutboxMessage
from app.domain.payment import Payment


class PaymentRepository(Protocol):
    async def insert_if_absent(self, payment: Payment) -> bool:
        """INSERT ... ON CONFLICT (idempotency_key) DO NOTHING. True if the row was inserted."""
        ...

    async def get_by_id(self, payment_id: UUID) -> Payment | None: ...

    async def get_by_idempotency_key(self, key: str) -> Payment | None: ...

    async def mark_processed(
        self, payment_id: UUID, status: PaymentStatus, processed_at: datetime
    ) -> bool:
        """Set final status only if the payment is still pending. True if updated.

        Raises InvalidStatusTransition if ``status`` is not final.
        """
        ...

    async def mark_webhook_sent(self, payment_id: UUID, sent_at: datetime) -> bool:
        """Set webhook_sent_at only if it is still NULL. True if updated."""
        ...


class OutboxRepository(Protocol):
    async def add(self, message: OutboxMessage) -> None:
        """Plain INSERT; a duplicate (event_type, aggregate_id) raises (it is a bug)."""
        ...

    async def fetch_unpublished(self, limit: int) -> list[OutboxMessage]:
        """Oldest unpublished rows, locked FOR UPDATE SKIP LOCKED until the UoW ends."""
        ...

    async def mark_published(self, ids: Sequence[int], published_at: datetime) -> None: ...

    async def register_failure(self, message_id: int, error: str) -> None:
        """attempts += 1, last_error = error (truncated)."""
        ...


class UnitOfWork(Protocol):
    # Read-only in the protocol so implementations may expose concrete repository types.
    @property
    def payments(self) -> PaymentRepository: ...

    @property
    def outbox(self) -> OutboxRepository: ...

    async def __aenter__(self) -> Self: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Rolls back unless commit() was called; always closes the session."""
        ...

    async def commit(self) -> None: ...

    async def rollback(self) -> None: ...


UnitOfWorkFactory = Callable[[], UnitOfWork]
