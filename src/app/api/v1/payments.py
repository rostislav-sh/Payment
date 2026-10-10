from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Header, Response, status

from app.api.dependencies import get_payment_service, verify_api_key
from app.api.errors import ErrorResponse, ValidationErrorResponse
from app.api.routing import DecimalJSONRoute
from app.api.v1.schemas import CreatePaymentRequest, PaymentCreatedResponse, PaymentResponse
from app.application.payment_service import PaymentService

REPLAY_HEADER = "Idempotent-Replayed"

# Responses shared by every route of the router.
_COMMON_RESPONSES: dict[int | str, dict[str, Any]] = {
    status.HTTP_401_UNAUTHORIZED: {
        "model": ErrorResponse,
        "description": "Missing or invalid X-API-Key",
    },
    status.HTTP_422_UNPROCESSABLE_CONTENT: {
        "model": ValidationErrorResponse,
        "description": "Invalid request",
    },
    status.HTTP_500_INTERNAL_SERVER_ERROR: {
        "model": ErrorResponse,
        "description": "Unexpected error (details are only in the server log)",
    },
}

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(verify_api_key)],
    responses=_COMMON_RESPONSES,
    route_class=DecimalJSONRoute,
)

PaymentServiceDep = Annotated[PaymentService, Depends(get_payment_service)]


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentCreatedResponse,
    summary="Create a payment",
    description=(
        "Creates a pending payment and queues it for processing. The result is delivered "
        "to `webhook_url`.\n\n"
        "Idempotent by `Idempotency-Key`: repeating the request with the same key and body "
        "returns the existing payment (current state) with header `Idempotent-Replayed: true`; "
        "the same key with a different body is rejected with 409."
    ),
    responses={
        status.HTTP_202_ACCEPTED: {
            "description": "Payment accepted (or replayed)",
            "headers": {
                REPLAY_HEADER: {
                    "description": "`true` if an existing payment was returned for the key",
                    "schema": {"type": "string", "enum": ["true"]},
                }
            },
        },
        status.HTTP_409_CONFLICT: {
            "model": ErrorResponse,
            "description": "Idempotency-Key already used with a different request body",
        },
    },
)
async def create_payment(
    payload: CreatePaymentRequest,
    idempotency_key: Annotated[
        str,
        Header(
            alias="Idempotency-Key",
            min_length=1,
            max_length=255,
            pattern=r"^[!-~]+$",  # printable ASCII, no spaces
            description="Client-generated unique key of this payment (e.g. a UUID).",
        ),
    ],
    response: Response,
    service: PaymentServiceDep,
) -> PaymentCreatedResponse:
    result = await service.create(payload.to_command(idempotency_key))
    if result.replayed:
        response.headers[REPLAY_HEADER] = "true"
    return PaymentCreatedResponse.from_domain(result.payment)


@router.get(
    "/{payment_id}",
    response_model=PaymentResponse,
    summary="Get a payment",
    description="Returns the current state of the payment.",
    responses={
        status.HTTP_404_NOT_FOUND: {"model": ErrorResponse, "description": "Payment not found"},
    },
)
async def get_payment(payment_id: UUID, service: PaymentServiceDep) -> PaymentResponse:
    return PaymentResponse.from_domain(await service.get(payment_id))
