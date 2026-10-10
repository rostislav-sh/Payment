"""Classification of database driver errors."""

from sqlalchemy import exc as sa_exc


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
