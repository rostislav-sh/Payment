"""Unit of Work port: one transaction per ``async with`` block, exposing the repositories."""

from collections.abc import Callable
from types import TracebackType
from typing import Protocol, Self

from app.application.ports.repositories import OutboxRepository, PaymentRepository


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


type UnitOfWorkFactory = Callable[[], UnitOfWork]
