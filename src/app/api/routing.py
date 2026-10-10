"""JSON bodies are parsed with ``parse_float=Decimal``.

The default ``json.loads`` turns ``1234567890123456.78`` into a float before validation, so a
money amount sent as a JSON number would silently lose cents (and change the idempotency
hash). With Decimal parsing the amount reaches Pydantic exactly as the client wrote it.
This is the FastAPI "custom Request and APIRoute class" pattern.
"""

import json
from collections.abc import Callable, Coroutine
from decimal import Decimal
from typing import Any

from fastapi import Request, Response
from fastapi.routing import APIRoute


class DecimalJSONRequest(Request):
    async def json(self) -> Any:
        if not hasattr(self, "_json"):
            self._json = json.loads(await self.body(), parse_float=Decimal)
        return self._json


class DecimalJSONRoute(APIRoute):
    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        handler = super().get_route_handler()

        async def decimal_json_handler(request: Request) -> Response:
            return await handler(DecimalJSONRequest(request.scope, request.receive))

        return decimal_json_handler
