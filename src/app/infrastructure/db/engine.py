"""Engine and session factory. Create one of each per process."""

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


def create_engine(url: str) -> AsyncEngine:
    # READ COMMITTED explicitly: idempotent creation relies on it (a concurrent
    # INSERT .. ON CONFLICT waits, then the next SELECT sees the committed row).
    return create_async_engine(url, pool_pre_ping=True, isolation_level="READ COMMITTED")


def create_sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    # expire_on_commit=False: entities stay readable after commit without a new SELECT.
    return async_sessionmaker(engine, expire_on_commit=False)
