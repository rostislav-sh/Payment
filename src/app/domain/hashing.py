import hashlib
import json

from app.domain.commands import CreatePaymentCommand
from app.domain.exceptions import InvalidPaymentData
from app.domain.money import to_cents


def compute_request_hash(cmd: CreatePaymentCommand) -> str:
    """sha256 (hex) of the normalized request body.

    Normalization makes semantically equal requests hash equally: amount is brought to
    two decimal places and JSON keys are sorted. Everything else is compared strictly
    (no Unicode normalization; 1 and 1.0 differ). webhook_url is expected to be already
    normalized by the API (``str(HttpUrl)``). The idempotency key is not included:
    it is what the hash is compared against.

    The hash is persisted, so changing the canonical form breaks idempotency for stored
    keys; a golden test pins it.

    Raises InvalidPaymentData if metadata is not plain JSON (non-str keys, NaN, Decimal, ...).
    """
    canonical = {
        "amount": str(to_cents(cmd.amount)),
        "currency": str(cmd.currency),
        "description": cmd.description,
        "metadata": cmd.metadata,
        "webhook_url": cmd.webhook_url,
    }
    try:
        payload = json.dumps(
            canonical,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:  # ValueError: NaN/Infinity with allow_nan=False
        raise InvalidPaymentData(f"metadata is not JSON-serializable: {exc}") from exc
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
