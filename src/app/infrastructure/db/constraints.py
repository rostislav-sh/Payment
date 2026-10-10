"""SQL fragments for table constraints."""

from collections.abc import Iterable
from enum import StrEnum


def in_list(column: str, values: Iterable[StrEnum]) -> str:
    """SQL for ``column IN (...)`` built from enum values, so literals are not duplicated."""
    return f"{column} IN ({', '.join(f"'{v.value}'" for v in values)})"
