from decimal import Decimal, InvalidOperation

CENT = Decimal("0.01")


def to_cents(amount: Decimal) -> Decimal:
    """Normalize a positive amount to exactly two decimal places (100 -> 100.00).

    Never rounds: fractions of a cent, non-finite or non-positive amounts raise ValueError.
    The API validates amounts first; this guards the domain invariant itself.
    """
    if not amount.is_finite():
        raise ValueError(f"amount must be a finite number: {amount}")
    try:
        quantized = amount.quantize(CENT)
    except InvalidOperation as exc:  # exceeds the decimal context precision
        raise ValueError(f"amount is too large: {amount}") from exc
    if quantized != amount:
        raise ValueError(f"amount has more than 2 decimal places: {amount}")
    if quantized <= 0:
        raise ValueError(f"amount must be positive: {amount}")
    return quantized
