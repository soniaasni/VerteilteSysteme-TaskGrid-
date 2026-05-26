"""
Zentrales Logging-Setup für alle Komponenten.
Format: [TIMESTAMP] [LEVEL] [COMPONENT] [request_id=...] [task_id=...] MESSAGE
"""

import logging
import sys


def get_logger(component: str) -> logging.Logger:
    logger = logging.getLogger(component)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        ))
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
    return logger


def log_event(logger: logging.Logger, level: str, event: str,
              request_id: str = None, task_id: str = None, **extra):
    """Strukturierter Log-Eintrag mit optionalen IDs."""
    parts = [f"event={event}"]
    if request_id:
        parts.append(f"request_id={request_id}")
    if task_id:
        parts.append(f"task_id={task_id}")
    for k, v in extra.items():
        parts.append(f"{k}={v}")
    getattr(logger, level)(" | ".join(parts))
