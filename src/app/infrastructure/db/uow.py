"""Unit of Work: one session and one transaction per ``async with`` block."""

from types import TracebackType
from typing import Self

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.application.errors import StorageUnavailable
from app.infrastructure.db.errors import is_connection_error
from app.infrastructure.db.repositories import SqlOutboxRepository, SqlPaymentRepository


class SqlUnitOfWork:
    """Single-use: create a new instance (via the factory) for every operation."""

    payments: SqlPaymentRepository
    outbox: SqlOutboxRepository

    def __init__(self, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
        self._sessionmaker = sessionmaker
        self._session: AsyncSession | None = None

    async def __aenter__(self) -> Self:
        if self._session is not None:
            raise RuntimeError("SqlUnitOfWork is single-use; create a new one")
        self._session = self._sessionmaker()
        self.payments = SqlPaymentRepository(self._session)
        self.outbox = SqlOutboxRepository(self._session)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        # close() rolls back whatever was not committed and returns the connection to the
        # pool. If close() itself fails, its error propagates and the block's exception
        # stays only in __context__ (unlikely: close() tolerates broken connections).
        await self._active_session().close()
        if exc is not None and is_connection_error(exc):
            raise StorageUnavailable(f"database unavailable: {type(exc).__name__}") from exc

    async def commit(self) -> None:
        await self._active_session().commit()

    async def rollback(self) -> None:
        await self._active_session().rollback()

    def _active_session(self) -> AsyncSession:
        if self._session is None:
            raise RuntimeError("SqlUnitOfWork is used outside of 'async with'")
        return self._session
