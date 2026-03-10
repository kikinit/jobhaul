"""Centralized logging configuration for jobhaul.

Provides a single ``get_logger`` helper so that every module gets a
consistently formatted logger writing to stderr.
"""

import logging
import sys


def get_logger(name: str = "jobhaul") -> logging.Logger:
    """Return a named logger with a stderr handler and timestamp formatting.

    On the first call for a given *name* a ``StreamHandler`` is attached;
    subsequent calls return the same logger without adding duplicate
    handlers.

    Args:
        name: Logger name, typically ``__name__`` of the calling module.

    Returns:
        A ``logging.Logger`` instance ready for use.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
