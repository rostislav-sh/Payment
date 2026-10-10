"""Liveness and readiness probes (no authentication)."""

import asyncio
import logging
from collections.abc import Awaitable, Callable

from fastapi import APIRouter, Request, status
from fastapi.responses import JSONResponse

from app.api.errors import error_response
from app.api.schemas import ErrorResponse, StatusResponse

logger = logging.getLogger(__name__)

READY_TIMEOUT_SECONDS = 3.0

router = APIRouter(tags=["health"])

DatabaseProbe = Callable[[], Awaitable[None]]


@router.get(
    "/health",
    response_model=StatusResponse,
    summary="Liveness probe",
    description="Returns 200 while the process is up. Touches neither the database nor the broker.",
)
async def health() -> StatusResponse:
    return StatusResponse(status="ok")


@router.get(
    "/ready",
    response_model=StatusResponse,
    summary="Readiness probe",
    description=(
        "Runs `SELECT 1` against the database. The broker is not checked: "
        "the API does not depend on it (events go through the outbox)."
    ),
    responses={
        status.HTTP_503_SERVICE_UNAVAILABLE: {
            "model": ErrorResponse,
            "description": "Database is not available",
        }
    },
)
async def ready(request: Request) -> StatusResponse | JSONResponse:
    probe: DatabaseProbe = request.app.state.database_probe
    try:
        async with asyncio.timeout(READY_TIMEOUT_SECONDS):
            await probe()
    except Exception as exc:
        # Only the type: driver messages may contain connection details.
        logger.warning("readiness check failed error=%s", type(exc).__name__)
        return error_response(
            status.HTTP_503_SERVICE_UNAVAILABLE, "not_ready", "Database is not available"
        )
    return StatusResponse(status="ready")
