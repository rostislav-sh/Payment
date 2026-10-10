"""Outbox relay process: ``python -m app.entrypoints.relay``.

Polls the outbox, publishes events to RabbitMQ and marks them published. Several relays can
run in parallel (FOR UPDATE SKIP LOCKED). SIGTERM/SIGINT finish the current batch, then exit.
On Windows (no signal handlers) Ctrl+C cancels the batch instead: its uncommitted events are
published again later, which at-least-once delivery allows.
"""

import asyncio
import contextlib
import logging
import signal

from app.application.outbox_relay import OutboxRelay
from app.application.results import RelayBatchResult
from app.core.config import (
    get_broker_settings,
    get_database_settings,
    get_logging_settings,
    get_relay_settings,
)
from app.core.logging import setup_logging
from app.infrastructure.broker.connection import make_broker
from app.infrastructure.broker.publisher import RabbitEventPublisher
from app.infrastructure.broker.topology import declare_topology
from app.infrastructure.db.engine import create_engine, create_sessionmaker
from app.infrastructure.db.uow import SqlUnitOfWork

logger = logging.getLogger(__name__)

MAX_BACKOFF_SECONDS = 5.0


class Backoff:
    """Pause after failures: starts at the poll interval, doubles up to MAX_BACKOFF_SECONDS."""

    def __init__(self, initial: float) -> None:
        self._initial = initial
        self._current = initial

    def next(self) -> float:
        delay = self._current
        self._current = min(self._current * 2, MAX_BACKOFF_SECONDS)
        return delay

    def reset(self) -> None:
        self._current = self._initial


async def run(relay: OutboxRelay, stop: asyncio.Event, poll_interval: float) -> None:
    backoff = Backoff(poll_interval)
    while not stop.is_set():
        try:
            result = await relay.run_once()
        except Exception:
            # Database errors and the like: keep the process alive and retry later.
            logger.exception("relay iteration failed")
            delay = backoff.next()
        else:
            delay = _next_delay(result, relay.batch_size, poll_interval, backoff)
        if delay:
            # Sleep, but wake up immediately on shutdown.
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), delay)


def _next_delay(
    result: RelayBatchResult, batch_size: int, poll_interval: float, backoff: Backoff
) -> float:
    if result.aborted or (result.fetched and not result.published):
        # Broker down, unexpected error, or every event rejected: re-reading the same batch
        # at once would only spin, so back off.
        return backoff.next()
    backoff.reset()
    if result.fetched == batch_size:
        return 0.0  # a full batch went out: more events are probably waiting
    return poll_interval


def _install_signal_handlers(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        # Windows event loops have no signal handlers: there Ctrl+C cancels asyncio.run and
        # the finally block in main() still closes the connections.
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)


async def main() -> None:
    setup_logging(get_logging_settings().log_level)
    settings = get_relay_settings()
    engine = create_engine(get_database_settings().database_url.get_secret_value())
    broker = make_broker(get_broker_settings().rabbitmq_url.get_secret_value())
    stop = asyncio.Event()
    _install_signal_handlers(stop)
    try:
        await broker.connect()
        await declare_topology(broker)
        sessionmaker = create_sessionmaker(engine)
        relay = OutboxRelay(
            uow_factory=lambda: SqlUnitOfWork(sessionmaker),
            publisher=RabbitEventPublisher(broker),
            batch_size=settings.relay_batch_size,
        )
        logger.info("relay started batch_size=%d", settings.relay_batch_size)
        await run(relay, stop, settings.relay_poll_interval)
    finally:
        await broker.stop()
        await engine.dispose()
        logger.info("relay stopped")


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):  # Ctrl+C on Windows: no traceback
        asyncio.run(main())
