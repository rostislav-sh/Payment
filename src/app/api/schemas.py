"""Response models shared by all API versions (errors, probes); used for OpenAPI docs."""

from typing import Literal

from pydantic import BaseModel, Field


class ErrorDetail(BaseModel):
    field: str = Field(examples=["body.amount"])
    message: str = Field(examples=["Input should be greater than 0"])


class ErrorBody(BaseModel):
    code: str = Field(examples=["payment_not_found"])
    message: str


class ValidationErrorBody(ErrorBody):
    details: list[ErrorDetail]


class ErrorResponse(BaseModel):
    """Error body of every non-2xx response except validation errors."""

    error: ErrorBody


class ValidationErrorResponse(BaseModel):
    """Error body of a 422 caused by request validation."""

    error: ValidationErrorBody


class StatusResponse(BaseModel):
    status: Literal["ok", "ready"] = Field(examples=["ok"])
