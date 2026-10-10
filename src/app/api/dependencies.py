import secrets
from typing import Annotated

from fastapi import Depends, Request, Security
from fastapi.security import APIKeyHeader

from app.api.errors import Unauthorized
from app.application.interfaces import UnitOfWorkFactory
from app.application.payment_service import PaymentService
from app.infrastructure.db.uow import SqlUnitOfWork

# auto_error=False: a missing header reaches verify_api_key, so every auth failure gets the
# same 401 body. The scheme also adds the "Authorize" button to Swagger UI.
api_key_header = APIKeyHeader(
    name="X-API-Key",
    scheme_name="ApiKeyAuth",
    auto_error=False,
    description="Static API key of the client.",
)


async def verify_api_key(
    request: Request, api_key: Annotated[str | None, Security(api_key_header)] = None
) -> None:
    expected: str = request.app.state.api_key
    # Constant-time comparison; bytes, because compare_digest rejects non-ASCII str.
    if api_key is None or not secrets.compare_digest(api_key.encode(), expected.encode()):
        raise Unauthorized


def get_uow_factory(request: Request) -> UnitOfWorkFactory:
    sessionmaker = request.app.state.sessionmaker
    return lambda: SqlUnitOfWork(sessionmaker)


def get_payment_service(
    uow_factory: Annotated[UnitOfWorkFactory, Depends(get_uow_factory)],
) -> PaymentService:
    return PaymentService(uow_factory)
