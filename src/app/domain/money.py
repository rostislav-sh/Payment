from decimal import Decimal, InvalidOperation

from app.domain.exceptions import InvalidPaymentData

CENT = Decimal("0.01")


def to_cents(amount: Decimal) -> Decimal:
    """Normalize a positive amount to exactly two decimal places (100 -> 100.00).

    Never rounds: fractions of a cent, non-finite or non-positive amounts raise
    InvalidPaymentData.
    The API validates amounts first; this guards the domain invariant itself.
    """
    if not amount.is_finite():
        raise InvalidPaymentData(f"amount must be a finite number: {amount}")
    try:
        quantized = amount.quantize(CENT)
    except InvalidOperation as exc:  # exceeds the decimal context precision
        raise InvalidPaymentData(f"amount is too large: {amount}") from exc
    if quantized != amount:
        raise InvalidPaymentData(f"amount has more than 2 decimal places: {amount}")
    if quantized <= 0:
        raise InvalidPaymentData(f"amount must be positive: {amount}")
    return quantized
