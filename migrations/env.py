import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

from app.core.config import get_database_settings
from app.infrastructure.db import models

config = context.config

# Programmatic callers (tests) set configure_logger=False to keep their own logging.
# disable_existing_loggers=False: never silence loggers created before migrations ran.
if config.config_file_name is not None and config.attributes.get("configure_logger", True):
    fileConfig(config.config_file_name, disable_existing_loggers=False)

target_metadata = models.Base.metadata


def get_url() -> str:
    """URL comes from app settings, not from alembic.ini.

    Tests may pass an explicit URL via ``config.attributes["database_url"]``.
    Not using ``set_main_option``: configparser would choke on ``%`` in passwords.
    """
    url = config.attributes.get("database_url")
    if url:
        return str(url)
    return get_database_settings().database_url.get_secret_value()


def run_migrations_offline() -> None:
    context.configure(
        url=get_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)

    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    connectable = create_async_engine(get_url(), poolclass=pool.NullPool)

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


def run_migrations_online() -> None:
    # Alembic cookbook: a caller already inside an event loop (async tests) passes a sync
    # connection via ``conn.run_sync(...)`` instead of letting env.py call asyncio.run().
    connection = config.attributes.get("connection")
    if connection is not None:
        do_run_migrations(connection)
    else:
        asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
