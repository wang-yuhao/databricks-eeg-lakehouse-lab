"""Structured logging utilities for the EEG Lakehouse Lab.

Uses loguru for structured, leveled logging with automatic context injection.
All pipeline functions should import `get_logger` and call it at module start.
"""

import sys
from loguru import logger as _logger


def get_logger(name: str):
    """Return a configured logger with context bound to the calling module.

    Args:
        name: Module name, typically ``__name__``.

    Returns:
        A loguru logger instance with the module name bound.

    Example::

        from src.utils.logging import get_logger
        log = get_logger(__name__)
        log.info("Loading Bronze table", table="eeg_lakehouse.bronze.raw_eeg_files")
    """
    _logger.remove()  # Remove default stderr handler
    _logger.add(
        sys.stdout,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {extra[module]} | {message}",
        level="INFO",
        colorize=True,
    )
    return _logger.bind(module=name)
