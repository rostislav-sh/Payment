"""FastAPI application factory.

Run: ``uvicorn app.api.app:create_app --factory``.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from functools import partial

from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncEngine

from app.api import health
from app.api.errors import register_error_handlers
from app.api.v1 import payments
from app.core.config import get_api_settings, get_database_settings, get_logging_settings
from app.core.logging import setup_logging
from app.infrastructure.db.engine import create_engine, create_sessionmaker, ping


def create_app(*, api_key: str | None = None, engine: AsyncEngine | None = None) -> FastAPI:
    """``api_key`` defaults to API_KEY from settings. An injected ``engine`` (tests) is not
    disposed by the app, and logging is then left as configured by the caller."""
    if api_key is None:
        api_key = get_api_settings().api_key.get_secret_value()
    if not api_key:
        raise ValueError("API key must not be empty")

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if engine is None:
            setup_logging(get_logging_settings().log_level)
            app_engine = create_engine(get_database_settings().database_url.get_secret_value())
        else:
            app_engine = engine
        app.state.sessionmaker = create_sessionmaker(app_engine)
        app.state.database_probe = partial(ping, app_engine)
        try:
            yield
        finally:
            if engine is None:
                await app_engine.dispose()

    app = FastAPI(
        title="Payment Service",
        version="0.1.0",
        description=(
            "Asynchronous payment processing. Payments are created via the API, processed "
            "in the background, and the result is sent to the client's webhook.\n\n"
            "All `/api/v1` endpoints require the `X-API-Key` header."
        ),
        lifespan=lifespan,
    )
    app.state.api_key = api_key
    register_error_handlers(app)
    app.include_router(health.router)
    app.include_router(payments.router)
    return app
