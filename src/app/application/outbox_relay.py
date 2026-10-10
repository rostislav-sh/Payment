"""Outbox relay use case: publish unpublished outbox events to the broker.

At-least-once: an event is marked published only after the broker confirmed it. If the commit
that marks it fails, the event is published again on the next run, so consumers must be
idempotent (the payment consumer is).
"""

import logging

from app.application.clock import Clock, utc_now
from app.application.error_text import describe_error
from app.application.errors import BrokerUnavailable, EventRejected
from app.application.ports.publisher import EventPublisher
from app.application.ports.uow import UnitOfWorkFactory
from app.application.results import RelayBatchResult

logger = logging.getLogger(__name__)


class OutboxRelay:
    def __init__(
        self,
        uow_factory: UnitOfWorkFactory,
        publisher: EventPublisher,
        batch_size: int,
        clock: Clock = utc_now,
    ) -> None:
        self._uow_factory = uow_factory
        self._publisher = publisher
        self._batch_size = batch_size
        self._clock = clock

    @property
    def batch_size(self) -> int:
        return self._batch_size

    async def run_once(self) -> RelayBatchResult:
        """Publish one batch of the oldest unpublished events.

        The rows stay locked (FOR UPDATE SKIP LOCKED) and a pooled connection stays busy until
        the commit, i.e. while the batch is being published; publishing is bounded by a timeout.
        Several relays can run side by side without publishing the same event twice in normal
        operation.
        """
        async with self._uow_factory() as uow:
            messages = await uow.outbox.fetch_unpublished(self._batch_size)
            if not messages:
                return RelayBatchResult(fetched=0, published=0, failed=0, aborted=False)

            published: list[int] = []
            failed = 0
            aborted = False
            for message in messages:
                assert message.id is not None  # rows read from the outbox always have an id
                try:
                    await self._publisher.publish(message)
                except EventRejected as exc:
                    failed += 1
                    await uow.outbox.register_failure(message.id, describe_error(exc))
                    logger.warning(
                        "outbox event rejected message_id=%s error=%s",
                        message.id,
                        describe_error(exc),
                    )
                except BrokerUnavailable as exc:
                    # The rest of the batch would fail the same way: stop and let the caller
                    # back off. Unattempted events are simply picked up next time.
                    failed += 1
                    aborted = True
                    await uow.outbox.register_failure(message.id, describe_error(exc))
                    logger.warning(
                        "broker unavailable, batch stopped message_id=%s error=%s",
                        message.id,
                        describe_error(exc),
                    )
                    break
                except Exception as exc:
                    # Unexpected (a bug, an unmapped library error). Stop the batch but still
                    # commit what the broker already confirmed; otherwise those events would be
                    # republished on every run.
                    failed += 1
                    aborted = True
                    await uow.outbox.register_failure(message.id, describe_error(exc))
                    logger.exception(
                        "unexpected publish error, batch stopped message_id=%s", message.id
                    )
                    break
                else:
                    published.append(message.id)

            await uow.outbox.mark_published(published, self._clock())
            await uow.commit()

        if published:
            logger.info("outbox events published count=%d", len(published))
        return RelayBatchResult(
            fetched=len(messages),
            published=len(published),
            failed=failed,
            aborted=aborted,
        )
