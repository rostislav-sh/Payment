"""Exception -> HTTP mapping with one error body format for every response.

Body: ``{"error": {"code": str, "message": str}}``; validation errors add
``"details": [{"field": str, "message": str}, ...]``.
"""

import logging
from collections.abc import Mapping, Sequence
from http import HTTPStatus
from typing import Any

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.domain.exceptions import IdempotencyConflict, InvalidPaymentData, PaymentNotFound

logger = logging.getLogger(__name__)

VALIDATION_ERROR = "validation_error"


class Unauthorized(Exception):
    """The X-API-Key header is missing or does not match."""


def error_response(
    status_code: int,
    code: str,
    message: str,
    *,
    details: Sequence[Mapping[str, str]] | None = None,
    headers: Mapping[str, str] | None = None,
) -> JSONResponse:
    error: dict[str, Any] = {"code": code, "message": message}
    if details is not None:
        error["details"] = list(details)
    return JSONResponse({"error": error}, status_code=status_code, headers=headers)


async def _unauthorized(request: Request, exc: Exception) -> JSONResponse:
    return error_response(
        status.HTTP_401_UNAUTHORIZED,
        "unauthorized",
        "Missing or invalid API key",
        headers={"WWW-Authenticate": "APIKey"},
    )


async def _payment_not_found(request: Request, exc: Exception) -> JSONResponse:
    return error_response(status.HTTP_404_NOT_FOUND, "payment_not_found", str(exc))


async def _idempotency_conflict(request: Request, exc: Exception) -> JSONResponse:
    return error_response(status.HTTP_409_CONFLICT, "idempotency_conflict", str(exc))


async def _invalid_payment_data(request: Request, exc: Exception) -> JSONResponse:
    # Pydantic validates first; this is the domain rejecting what slipped through (NaN in
    # metadata). Same shape as other validation errors: details are always present.
    return error_response(
        status.HTTP_422_UNPROCESSABLE_CONTENT,
        VALIDATION_ERROR,
        "Request validation failed: body",
        details=[{"field": "body", "message": str(exc)}],
    )


async def _request_validation(request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, RequestValidationError)
    details = [
        {"field": ".".join(str(part) for part in err["loc"]), "message": err["msg"]}
        for err in exc.errors()
    ]
    fields = list(dict.fromkeys(d["field"] for d in details))
    message = "Request validation failed"
    if fields:
        message += ": " + ", ".join(fields)
    return error_response(
        status.HTTP_422_UNPROCESSABLE_CONTENT, VALIDATION_ERROR, message, details=details
    )


_HTTP_CODES = {
    status.HTTP_404_NOT_FOUND: "not_found",
    status.HTTP_405_METHOD_NOT_ALLOWED: "method_not_allowed",
}


async def _http_exception(request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, StarletteHTTPException)
    if isinstance(exc.detail, str) and exc.detail:
        message = exc.detail
    else:
        message = HTTPStatus(exc.status_code).phrase
    return error_response(
        exc.status_code,
        _HTTP_CODES.get(exc.status_code, "http_error"),
        message,
        headers=exc.headers,  # e.g. Allow on 405
    )


async def _unhandled(request: Request, exc: Exception) -> JSONResponse:
    # Full traceback goes to the log only; the client gets a generic message.
    logger.exception("unhandled error method=%s path=%s", request.method, request.url.path)
    return error_response(
        status.HTTP_500_INTERNAL_SERVER_ERROR, "internal_error", "Internal server error"
    )


def register_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(Unauthorized, _unauthorized)
    app.add_exception_handler(PaymentNotFound, _payment_not_found)
    app.add_exception_handler(IdempotencyConflict, _idempotency_conflict)
    app.add_exception_handler(InvalidPaymentData, _invalid_payment_data)
    app.add_exception_handler(RequestValidationError, _request_validation)
    app.add_exception_handler(StarletteHTTPException, _http_exception)
    # Exception is handled by Starlette's ServerErrorMiddleware (which then re-raises it,
    # so the server also sees the failure).
    app.add_exception_handler(Exception, _unhandled)
