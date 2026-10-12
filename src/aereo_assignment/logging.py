"""Logging setup with UTC timestamps."""

from __future__ import annotations

import logging
import time

from aereo_assignment.config import Settings


class _UtcFormatter(logging.Formatter):
    """Log formatter that renders timestamps in UTC."""

    converter = time.gmtime


def setup_logging(settings: Settings) -> None:
    """Configure stdlib logging using ``settings.log_level``."""
    handler = logging.StreamHandler()
    handler.setFormatter(
        _UtcFormatter(
            fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S%z",
        )
    )
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(settings.log_level.upper())
