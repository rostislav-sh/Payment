from typing import Any

import pytest
from alembic.autogenerate import compare_metadata
from alembic.runtime.migration import MigrationContext
from sqlalchemy import Connection, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from app.infrastructure.db.models import Base
from tests.integration.support import downgrade_base, upgrade_head

pytestmark = pytest.mark.integration


async def _tables_exist(conn: AsyncConnection) -> bool:
    result = await conn.execute(
        text("SELECT to_regclass('payments') IS NOT NULL AND to_regclass('outbox') IS NOT NULL")
    )
    return bool(result.scalar_one())


async def test_downgrade_base_then_upgrade_head(engine: AsyncEngine) -> None:
    # Always leaves the schema at head, even if downgrade fails half way.
    try:
        async with engine.begin() as conn:
            await conn.run_sync(downgrade_base)
            assert not await conn.scalar(text("SELECT to_regclass('payments') IS NOT NULL"))
            assert not await conn.scalar(text("SELECT to_regclass('outbox') IS NOT NULL"))
    finally:
        async with engine.begin() as conn:
            await conn.run_sync(upgrade_head)

    async with engine.connect() as conn:
        assert await _tables_exist(conn)
        version = await conn.scalar(text("SELECT count(*) FROM alembic_version"))
        assert version == 1


def _schema_diff(connection: Connection) -> list[Any]:
    context = MigrationContext.configure(connection, opts={"compare_server_default": True})
    return list(compare_metadata(context, Base.metadata))


async def test_models_match_migrations(engine: AsyncEngine) -> None:
    # A model change without a migration (or vice versa) shows up here.
    async with engine.connect() as conn:
        assert await conn.run_sync(_schema_diff) == []
