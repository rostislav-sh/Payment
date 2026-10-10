"""Declare the RabbitMQ topology and exit: ``python -m app.entrypoints.declare_topology``.

Used for manual verification; the relay and the consumer declare it themselves on start.
"""

import asyncio
import logging

from app.core.config import get_broker_settings, get_logging_settings
from app.core.logging import setup_logging
from app.infrastructure.broker.connection import make_broker
from app.infrastructure.broker.topology import declare_topology

logger = logging.getLogger(__name__)


async def main() -> None:
    setup_logging(get_logging_settings().log_level)
    async with make_broker(get_broker_settings().rabbitmq_url.get_secret_value()) as broker:
        await declare_topology(broker)
    logger.info("topology declared")


if __name__ == "__main__":
    asyncio.run(main())
