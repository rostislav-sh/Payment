"""Logging setup shared by all processes (api, relay, consumer, webhook-mock).

Messages use a key=value style for context, e.g.
``logger.info("payment processed payment_id=%s status=%s", pid, status)``.
Secrets (API key, credentials in URLs) must never be passed to the logger.
"""

import logging
import sys

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


def setup_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(LOG_FORMAT))

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())

    # Keep noisy libraries at WARNING unless debugging.
    library_level = root.level if root.level <= logging.DEBUG else logging.WARNING
    for name in ("aiormq", "aio_pika", "httpx", "httpcore"):
        logging.getLogger(name).setLevel(library_level)
