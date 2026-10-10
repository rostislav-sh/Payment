"""Demo webhook receiver: ``uvicorn app.entrypoints.webhook_mock:app --port 8001`` locally
(port 8080 inside docker compose, published as localhost:8001).

Stands in for the client's endpoint:

* ``POST /hook`` answers 200 and records the notification;
* ``POST /hook?fail=500`` (any 200-599) always answers that code: retries and the DLQ can be
  shown without restarting anything (500 -> 3 retries -> DLQ, 404 -> DLQ at once);
* WEBHOOK_MOCK_FAIL_RATE (0..1) answers 500 at random, for flaky-endpoint demos;
* ``GET /received`` lists the latest notifications, ``DELETE /received`` clears them.

Each record shows whether the Idempotency-Key was seen before: delivery is at-least-once, and
this is how a real client deduplicates.

State lives in process memory: run a single process (no ``--workers``).
"""

import logging
import random
from collections import deque
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Annotated, Any

from fastapi import FastAPI, Header, Query, Request, Response, status
from pydantic import BaseModel

from app.core.config import get_logging_settings, get_webhook_mock_settings
from app.core.logging import setup_logging

logger = logging.getLogger(__name__)

MAX_RECORDS = 100
MAX_SEEN_KEYS = 10_000  # bounds memory under load; the oldest keys are forgotten first


class ReceivedWebhook(BaseModel):
    received_at: datetime
    idempotency_key: str | None
    duplicate: bool  # this Idempotency-Key was received before
    answered: int  # status code the mock replied with
    body: Any


def create_app() -> FastAPI:
    fail_rate = get_webhook_mock_settings().webhook_mock_fail_rate
    records: deque[ReceivedWebhook] = deque(maxlen=MAX_RECORDS)
    seen_keys: dict[str, None] = {}  # insertion-ordered set
    rng = random.Random()  # demo randomness only

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        setup_logging(get_logging_settings().log_level)
        logger.info("webhook mock started fail_rate=%s", fail_rate)
        yield

    app = FastAPI(title="Webhook mock", version="0.1.0", lifespan=lifespan)

    @app.post("/hook", summary="Receive a payment webhook")
    async def receive(
        request: Request,
        # 1xx is not a final response: the sender would wait for another one until it times out.
        fail: Annotated[int | None, Query(ge=200, le=599)] = None,
        idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
    ) -> Response:
        try:
            body: Any = await request.json()
        except ValueError:
            body = (await request.body()).decode("utf-8", "replace")

        answer = fail or status.HTTP_200_OK
        if fail is None and rng.random() < fail_rate:
            answer = status.HTTP_500_INTERNAL_SERVER_ERROR

        duplicate = idempotency_key is not None and idempotency_key in seen_keys
        if idempotency_key is not None and 200 <= answer < 300:
            seen_keys[idempotency_key] = None  # only a 2xx counts as delivered
            if len(seen_keys) > MAX_SEEN_KEYS:
                del seen_keys[next(iter(seen_keys))]
        records.appendleft(
            ReceivedWebhook(
                received_at=datetime.now(UTC),
                idempotency_key=idempotency_key,
                duplicate=duplicate,
                answered=answer,
                body=body,
            )
        )
        logger.info(
            "webhook received key=%s status=%s answered=%d duplicate=%s",
            idempotency_key,
            body.get("status") if isinstance(body, dict) else None,
            answer,
            duplicate,
        )
        return Response(status_code=answer)

    @app.get("/received", summary="Latest received webhooks, newest first")
    async def received() -> list[ReceivedWebhook]:
        return list(records)

    @app.delete("/received", status_code=status.HTTP_204_NO_CONTENT, summary="Forget everything")
    async def clear() -> None:
        records.clear()
        seen_keys.clear()

    @app.get("/health", summary="Liveness probe")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


app = create_app()
