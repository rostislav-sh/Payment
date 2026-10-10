from uuid import UUID


class DomainError(Exception):
    """Base class for domain errors. Mapped to HTTP codes only in the API layer."""


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
