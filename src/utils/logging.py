"""Structured logging utilities for the EEG Lakehouse pipeline.

Provides a consistent logger factory used across all pipeline modules.
In Databricks, logs appear in the cluster driver logs and can be forwarded
to Azure Monitor / Log Analytics (not covered in Day 1 scope).
"""

import logging
import sys
from typing import Optional


def get_logger(name: str, level: Optional[int] = None) -> logging.Logger:
    """Return a configured logger for the given module name.

    Args:
        name: Typically pass ``__name__`` so logs show the module path.
        level: Override log level (default: INFO).

    Returns:
        Configured :class:`logging.Logger` instance.

    Example::

        from src.utils.logging import get_logger
        logger = get_logger(__name__)
        logger.info("Starting Bronze ingestion", extra={"subject_count": 197})
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.setLevel(level or logging.INFO)
    return logger
