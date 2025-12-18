from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

from loguru import logger


def configure_logging(log_level: str = "INFO", log_path: Optional[Path] = None) -> None:
    """Configure loguru logging with optional file sink."""
    logger.remove()
    logger.add(
        sys.stdout,
        level=log_level.upper(),
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
            "<level>{message}</level>"
        ),
        enqueue=True,
    )

    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        logger.add(log_path, level=log_level.upper(), rotation="10 MB", enqueue=True)


__all__ = ["configure_logging", "logger"]

