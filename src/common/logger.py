"""
Zentrales Logging-Setup für alle Komponenten.

Format (gemäß §13 Aufgabenstellung):
  [component] request_id=... task_id=... event=... key=value ...

Beispiel:
  [dispatcher] request_id=abc123 task_id=42 event=POST_TASK_accepted status=QUEUED type=sum
  [dispatcher] request_id=abc123 task_id=42 event=DISPATCH_sending status=DISPATCHED worker=worker-sum-1
  [dispatcher] request_id=abc123 task_id=42 event=RESULT_RETURN_stored status=COMPLETED duration_ms=37

Logs werden gleichzeitig auf stdout und in eine Datei geschrieben.
Log-Verzeichnis: LOG_DIR Umgebungsvariable (Standard: logs/)
"""

import logging
import os
import sys
from pathlib import Path


def _get_file_handler(component: str) -> logging.FileHandler:
    log_dir = Path(os.environ.get("LOG_DIR", "logs"))  # dynamisch lesen
    log_dir.mkdir(parents=True, exist_ok=True)
    safe_name = component.replace(".", "_").replace("/", "_")
    log_path = log_dir / f"{safe_name}.log"
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(_make_formatter(with_timestamp=True))
    return handler


def _make_formatter(with_timestamp: bool = False) -> logging.Formatter:
    if with_timestamp:
        fmt = "%(asctime)s [%(name)s] %(levelname)s %(message)s"
    else:
        fmt = "[%(name)s] %(message)s"
    return logging.Formatter(fmt=fmt, datefmt="%Y-%m-%dT%H:%M:%S")


def get_logger(component: str) -> logging.Logger:
    logger = logging.getLogger(component)
    if not logger.handlers:
        # Console: kompaktes Format wie in §13 Beispiel
        console = logging.StreamHandler(sys.stdout)
        console.setFormatter(_make_formatter(with_timestamp=False))
        logger.addHandler(console)

        # Datei: mit Timestamp für Testprotokoll-Nachvollziehbarkeit
        try:
            logger.addHandler(_get_file_handler(component))
        except OSError:
            pass   # Kein Schreibzugriff im Test → nur Console

        logger.setLevel(logging.DEBUG)
    return logger


def log_event(logger: logging.Logger, level: str, event: str,
              request_id: str = None, task_id: str = None, **extra):
    """
    Strukturierter Log-Eintrag gemäß §13.
    Reihenfolge: request_id task_id event <weitere Felder>

    Beispiel-Output:
      request_id=abc123 task_id=42 event=POST_TASK_accepted status=QUEUED type=sum
    """
    parts = []
    if request_id:
        parts.append(f"request_id={request_id}")
    if task_id:
        parts.append(f"task_id={task_id}")
    parts.append(f"event={event}")
    for k, v in extra.items():
        parts.append(f"{k}={v}")
    getattr(logger, level)(" ".join(parts))
