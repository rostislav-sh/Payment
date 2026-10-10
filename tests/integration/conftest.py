"""Integration fixtures: one Postgres container per session, migrated with Alembic.

Every test starts with empty tables (TRUNCATE ... RESTART IDENTITY), so outbox ids are
predictable (1, 2, 3, ...).

RabbitMQ (``rabbitmq`` fixture) is a separate session container, started lazily: only runs
that contain a test requesting it pay for its startup.
"""

from collections.abc import AsyncIterator, Iterator

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from testcontainers.community.postgres import PostgresContainer
from testcontainers.core.container import DockerContainer

from app.application.interfaces import UnitOfWorkFactory
from app.infrastructure.db.engine import create_engine, create_sessionmaker
from app.infrastructure.db.uow import SqlUnitOfWork
from tests.integration.support import (
    RABBITMQ_IMAGE,
    RABBITMQ_PASSWORD,
    RABBITMQ_USER,
    RabbitMQEndpoints,
    UowFactory,
    upgrade_head,
    wait_for_rabbitmq,
)


@pytest.fixture(scope="session")
def database_url() -> Iterator[str]:
    with PostgresContainer("postgres:18", driver="asyncpg") as postgres:
        yield postgres.get_connection_url()


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def engine(database_url: str) -> AsyncIterator[AsyncEngine]:
    engine = create_engine(database_url)
    async with engine.begin() as conn:
        await conn.run_sync(upgrade_head)
    yield engine
    await engine.dispose()


@pytest.fixture(scope="session")
def sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return create_sessionmaker(engine)


@pytest.fixture
def uow_factory(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> UowFactory:
    factory: UowFactory = lambda: SqlUnitOfWork(sessionmaker)  # noqa: E731
    # Static check (mypy): the SQL implementation satisfies the application port.
    _port_check: UnitOfWorkFactory = factory
    return factory


@pytest_asyncio.fixture(autouse=True, loop_scope="session")
async def _clean_tables(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        # Fail fast instead of hanging if a previous test leaked an open transaction.
        await conn.execute(text("SET LOCAL lock_timeout = '5s'"))
        await conn.execute(text("TRUNCATE payments, outbox RESTART IDENTITY"))


@pytest.fixture(scope="session")
def _rabbitmq_container() -> Iterator[RabbitMQEndpoints]:
    with DockerContainer(RABBITMQ_IMAGE).with_exposed_ports(5672, 15672) as container:
        host = container.get_container_host_ip()
        amqp_port = container.get_exposed_port(5672)
        http_port = container.get_exposed_port(15672)
        yield RabbitMQEndpoints(
            amqp_url=f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASSWORD}@{host}:{amqp_port}/",
            management_url=f"http://{host}:{http_port}",
        )


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def rabbitmq(_rabbitmq_container: RabbitMQEndpoints) -> RabbitMQEndpoints:
    """A ready RabbitMQ 4.3 (AMQP + management API), one per session."""
    await wait_for_rabbitmq(_rabbitmq_container)
    return _rabbitmq_container
