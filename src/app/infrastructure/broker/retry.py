"""Where a failed payment message goes next: a delayed retry or the DLQ.

    transient error, retries_done < MAX_RETRIES -> payments.retry / retry.{retries_done+1}
                                                  (TTL 1/2/4 s, then back to payments.new)
    transient error, retries exhausted      -> payments.dlx, x-error-kind=retries_exhausted
    permanent error (or malformed message)  -> payments.dlx, x-error-kind=permanent

"3 attempts" from the assignment is read as the first delivery plus 3 retries (4 in total).

Headers (``x-attempt``, ``x-errors``, ...) and parsing of the incoming message live in
broker/failure_headers.py.

Every publish goes through publish_confirmed (persist, mandatory, confirm, timeout) and the
caller acks the incoming message only afterwards: a crash in between yields a harmless
duplicate, never a lost message. If publishing fails, the exception propagates: the subscriber
rejects the message if the channel is alive (the queue dead-letters it as a safety net, without
our headers), otherwise it is redelivered when the channel closes (counted by the delivery
limit).
"""

import logging
from datetime import UTC, datetime

from faststream.rabbit import RabbitBroker

from app.domain.exceptions import TransientProcessingError
from app.infrastructure.broker.failure_headers import (
    ATTEMPT_HEADER,
    ATTEMPTS_HEADER,
    ERROR_KIND_HEADER,
    ERRORS_HEADER,
    FAILED_AT_HEADER,
    MAX_ERROR_ENTRIES,
    ORIGINAL_ROUTING_KEY_HEADER,
    Delivery,
    FailureKind,
    error_entry,
)
from app.infrastructure.broker.publishing import publish_confirmed
from app.infrastructure.broker.topology import (
    DLX_EXCHANGE,
    MAX_RETRIES,
    PAYMENT_CREATED_ROUTING_KEY,
    RETRY_EXCHANGE,
    retry_routing_key,
)

logger = logging.getLogger(__name__)


class FailureRouter:
    def __init__(self, broker: RabbitBroker) -> None:
        self._broker = broker

    async def route(self, delivery: Delivery, error: Exception) -> None:
        """Publish the message to a retry queue or the DLQ; the caller acks afterwards."""
        retries_done = delivery.retries_done
        history = [*delivery.error_history, error_entry(retries_done + 1, error)]
        errors = history[-MAX_ERROR_ENTRIES:]

        if isinstance(error, TransientProcessingError) and retries_done < MAX_RETRIES:
            await self._retry(delivery, retries_done + 1, errors)
            return
        kind = (
            FailureKind.RETRIES_EXHAUSTED
            if isinstance(error, TransientProcessingError)
            else FailureKind.PERMANENT
        )
        await self._dead_letter(delivery, kind, retries_done + 1, errors, error)

    async def _retry(self, delivery: Delivery, retry: int, errors: list[str]) -> None:
        await publish_confirmed(
            self._broker,
            delivery.body,
            exchange=RETRY_EXCHANGE,
            routing_key=retry_routing_key(retry),
            message_id=delivery.message_id,
            headers={ATTEMPT_HEADER: retry, ERRORS_HEADER: errors},
            content_type=delivery.content_type,
        )
        logger.warning(
            "message scheduled for retry message_id=%s retry=%d/%d error=%s",
            delivery.message_id,
            retry,
            MAX_RETRIES,
            errors[-1],
        )

    async def _dead_letter(
        self,
        delivery: Delivery,
        kind: FailureKind,
        attempts: int,
        errors: list[str],
        error: Exception,
    ) -> None:
        # No x-attempt here: a message re-published from the DLQ starts from scratch.
        await publish_confirmed(
            self._broker,
            delivery.body,
            exchange=DLX_EXCHANGE,
            routing_key="",  # fanout
            message_id=delivery.message_id,
            headers={
                ERRORS_HEADER: errors,
                ATTEMPTS_HEADER: attempts,
                ERROR_KIND_HEADER: kind.value,
                FAILED_AT_HEADER: datetime.now(UTC).isoformat(),
                ORIGINAL_ROUTING_KEY_HEADER: PAYMENT_CREATED_ROUTING_KEY,
            },
            content_type=delivery.content_type,
        )
        # The traceback (with chained causes, e.g. the DB error behind StorageUnavailable)
        # goes to the log only; headers carry just the type and a short message.
        logger.error(
            "message moved to DLQ message_id=%s kind=%s attempts=%d error=%s",
            delivery.message_id,
            kind,
            attempts,
            errors[-1],
            exc_info=error,
        )
