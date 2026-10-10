"""HTTP implementation of the WebhookSender port (httpx)."""

import asyncio

import httpx

from app.application.ports.webhook import WebhookRejected, WebhookUnavailable
from app.domain.payment import Payment

WEBHOOK_EVENT = "payment.processed"
# Client errors that a later attempt may fix: request timeout and rate limiting.
RETRYABLE_CLIENT_ERRORS = frozenset({408, 429})


def build_payload(payment: Payment) -> dict[str, str | None]:
    return {
        "event": WEBHOOK_EVENT,
        "payment_id": str(payment.id),
        "status": payment.status.value,
        "amount": str(payment.amount),  # a string, like in the API: no float rounding
        "currency": payment.currency.value,
        "processed_at": payment.processed_at.isoformat() if payment.processed_at else None,
    }


class HttpWebhookSender:
    """One shared AsyncClient per process (connection pooling); created and closed by the
    consumer's lifespan. Redirects are not followed: a 3xx is treated as a client error."""

    def __init__(self, client: httpx.AsyncClient, timeout: float) -> None:
        self._client = client
        # httpx timeouts apply per phase and per read chunk; this bounds the whole delivery,
        # so a slow endpoint cannot hold a consumer slot past the shutdown grace period.
        self._timeout = timeout

    async def send(self, payment: Payment) -> None:
        try:
            async with (
                asyncio.timeout(self._timeout),
                # stream(): only the status line and headers are read, never a large body.
                self._client.stream(
                    "POST",
                    payment.webhook_url,
                    json=build_payload(payment),
                    # Same value on every delivery of this notification: lets the client dedupe.
                    headers={"Idempotency-Key": str(payment.id)},
                ) as response,
            ):
                status = response.status_code
        except (httpx.InvalidURL, httpx.UnsupportedProtocol, httpx.LocalProtocolError) as exc:
            # Our request cannot be sent as is; checked before TransportError (their parent
            # for the last two), because retries cannot help.
            raise WebhookRejected(f"webhook request cannot be sent: {type(exc).__name__}") from exc
        except (TimeoutError, httpx.TransportError) as exc:
            raise WebhookUnavailable(f"webhook not delivered: {type(exc).__name__}") from exc

        if 200 <= status < 300:
            return
        if status >= 500 or status in RETRYABLE_CLIENT_ERRORS:
            raise WebhookUnavailable(f"webhook endpoint answered {status}")
        # 1xx/3xx/other 4xx: the endpoint or URL is wrong; repeating will not change that.
        raise WebhookRejected(f"webhook endpoint answered {status}")
