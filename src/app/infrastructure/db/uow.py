"""Unit of Work: one session and one transaction per ``async with`` block."""

from types import TracebackType
from typing import Self

from sqlalchemy import exc as sa_exc
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.application.ports.uow import StorageUnavailable
from app.infrastructure.db.repositories import SqlOutboxRepository, SqlPaymentRepository


def is_connection_error(error: BaseException) -> bool:
    """Errors a later attempt can fix: the database is down, restarting or unreachable.

    Integrity/programming/data errors are bugs and are deliberately not included.
    """
    if isinstance(error, sa_exc.DBAPIError) and error.connection_invalidated:
        return True
    return isinstance(
        error,
        (
            sa_exc.OperationalError,
            sa_exc.InterfaceError,
            sa_exc.TimeoutError,  # connection pool exhausted
            OSError,  # asyncpg connect errors arrive unwrapped
            TimeoutError,
        ),
    )


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
        # pool; a failure here never masks the original exception from the block.
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
