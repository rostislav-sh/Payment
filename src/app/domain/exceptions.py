from uuid import UUID


class DomainError(Exception):
    """Base class for domain errors. Mapped to HTTP codes only in the API layer."""


class InvalidPaymentData(DomainError, ValueError):
    """Payment data breaks a domain invariant (amount, metadata).

    The API validates input first, so reaching this is unexpected; mapped to 422, not 500.
    """


class InvalidStatusTransition(DomainError, ValueError):
    """A status change the payment lifecycle does not allow (only pending -> final)."""


class NaiveDatetime(DomainError, ValueError):
    """A datetime without a timezone where the domain requires an aware one (UTC)."""


class PaymentNotFound(DomainError):
    def __init__(self, payment_id: UUID) -> None:
        super().__init__(f"payment {payment_id} not found")
        self.payment_id = payment_id


class IdempotencyConflict(DomainError):
    def __init__(self, idempotency_key: str) -> None:
        super().__init__("Idempotency-Key already used with a different request body")
        self.idempotency_key = idempotency_key


class ProcessingError(DomainError):
    """Consumer-side failure; the subclass decides between retry and DLQ."""


class PermanentProcessingError(ProcessingError):
    """A retry will not help (payment missing, webhook 4xx, malformed message) -> DLQ."""


class TransientProcessingError(ProcessingError):
    """Temporary failure (DB, network, timeout, webhook 5xx/408/429) -> retry."""


class PaymentMissing(PermanentProcessingError):
    """A payment event refers to a payment that does not exist (consumer side; the API uses
    PaymentNotFound). Payments are never deleted, so a retry cannot help."""

    def __init__(self, payment_id: UUID) -> None:
        super().__init__(f"payment {payment_id} does not exist")
        self.payment_id = payment_id
