"""Statement execution helpers shared by the repositories."""

from typing import Any, cast

from sqlalchemy import CursorResult, Update
from sqlalchemy.ext.asyncio import AsyncSession


async def execute_update(session: AsyncSession, stmt: Update) -> int:
    """Run a bulk UPDATE without touching the identity map; return the affected row count."""
    result = await session.execute(stmt.execution_options(synchronize_session=False))
    return cast(CursorResult[Any], result).rowcount
