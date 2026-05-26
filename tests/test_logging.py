"""
Tests für Issue #19: Strukturierte Logs mit request_id und task_id.
Prüft Format, File-Logging und Pflicht-Ereignisse gemäß §13 Aufgabenstellung.
"""

import logging
import os
import tempfile
from io import StringIO
from unittest.mock import patch, MagicMock

import pytest

from src.common.logger import get_logger, log_event


# ── Hilfsfunktionen ──────────────────────────────────────────────────────────

def _capture_log(logger_name: str, fn) -> str:
    """Ruft fn() auf und gibt alles zurück was auf dem Stream-Handler landet."""
    buf = StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(logging.Formatter("[%(name)s] %(message)s"))
    log = logging.getLogger(logger_name)
    log.setLevel(logging.DEBUG)
    log.propagate = False
    log.addHandler(handler)
    try:
        fn(log)
    finally:
        log.removeHandler(handler)
        log.propagate = True
    return buf.getvalue()


# ── Format-Tests ──────────────────────────────────────────────────────────────

def test_log_event_contains_request_id():
    out = _capture_log("test.fmt1", lambda l: log_event(l, "info", "TEST", request_id="req-1"))
    assert "request_id=req-1" in out


def test_log_event_contains_task_id():
    out = _capture_log("test.fmt2", lambda l: log_event(l, "info", "TEST", task_id="t-42"))
    assert "task_id=t-42" in out


def test_log_event_contains_event_name():
    out = _capture_log("test.fmt3", lambda l: log_event(l, "info", "MY_EVENT"))
    assert "event=MY_EVENT" in out


def test_log_event_request_id_before_task_id():
    out = _capture_log("test.fmt4", lambda l: log_event(
        l, "info", "E", request_id="r1", task_id="t1"))
    assert out.index("request_id=") < out.index("task_id=")


def test_log_event_ids_before_event():
    out = _capture_log("test.fmt5", lambda l: log_event(
        l, "info", "E", request_id="r1", task_id="t1"))
    assert out.index("task_id=") < out.index("event=")


def test_log_event_extra_fields():
    out = _capture_log("test.fmt6", lambda l: log_event(
        l, "info", "E", status="QUEUED", task_type="sum"))
    assert "status=QUEUED" in out
    assert "task_type=sum" in out


def test_log_event_no_pipes_in_output():
    out = _capture_log("test.fmt7", lambda l: log_event(
        l, "info", "E", request_id="r1", task_id="t1", status="QUEUED"))
    assert "|" not in out


def test_log_event_space_separated():
    out = _capture_log("test.fmt8", lambda l: log_event(
        l, "info", "E", request_id="r1", task_id="t1"))
    msg_part = out.split("] ", 1)[-1].strip()
    # Alle Felder durch Leerzeichen getrennt
    assert " " in msg_part
    for field in msg_part.split():
        assert "=" in field


def test_log_event_without_ids_still_works():
    out = _capture_log("test.fmt9", lambda l: log_event(l, "info", "NO_IDS"))
    assert "event=NO_IDS" in out


# ── File-Logging-Tests ────────────────────────────────────────────────────────

def test_log_file_created(tmp_path):
    with patch.dict(os.environ, {"LOG_DIR": str(tmp_path)}):
        # Neuen Logger erzwingen (nicht gecacht)
        name = "test.filelog.unique1"
        logging.getLogger(name).handlers.clear()
        logger = get_logger(name)
        log_event(logger, "info", "FILE_TEST", task_id="t1")

        log_files = list(tmp_path.glob("*.log"))
        assert len(log_files) >= 1


def test_log_file_contains_event(tmp_path):
    with patch.dict(os.environ, {"LOG_DIR": str(tmp_path)}):
        name = "test.filelog.unique2"
        logging.getLogger(name).handlers.clear()
        logger = get_logger(name)
        log_event(logger, "info", "WRITE_TEST", task_id="t99", status="QUEUED")

        log_files = list(tmp_path.glob("*.log"))
        content = log_files[0].read_text(encoding="utf-8")
        assert "event=WRITE_TEST" in content
        assert "task_id=t99" in content


def test_log_file_contains_timestamp(tmp_path):
    with patch.dict(os.environ, {"LOG_DIR": str(tmp_path)}):
        name = "test.filelog.unique3"
        logging.getLogger(name).handlers.clear()
        logger = get_logger(name)
        log_event(logger, "info", "TS_TEST")

        log_files = list(tmp_path.glob("*.log"))
        content = log_files[0].read_text(encoding="utf-8")
        # Timestamp-Format 2026-...
        assert "202" in content


# ── Pflicht-Ereignisse §13 ─────────────────────────────────────────────────────

def test_all_7_mandatory_events_present():
    """Prüft dass alle 7 Pflicht-Events im Code vorkommen."""
    import subprocess, sys
    result = subprocess.run(
        [sys.executable, "-m", "grep", "-r", "log_event", "src/dispatcher/"],
        capture_output=True, text=True, cwd=os.path.join(os.path.dirname(__file__), "..")
    )
    # Wir prüfen direkt die Event-Namen im Source
    import pathlib
    src = ""
    for f in pathlib.Path("src/dispatcher").rglob("*.py"):
        src += f.read_text(encoding="utf-8", errors="ignore")

    mandatory_events = [
        "POST_TASK_accepted",          # Task empfangen (QUEUED)
        "DISPATCH_sending",            # Task dispatcht (DISPATCHED + worker_id)
        "RESULT_RETURN_stored",        # Ergebnis empfangen (COMPLETED + duration_ms)
        "DISPATCH_TIMEOUT_fired",      # Timeout aufgetreten
        "TIMEOUT_retrying",            # Retry-Versuch (RETRYING + retry_count)
        "TIMEOUT_max_retries_exceeded",# Fehler (FAILED)
        "LOOKUP_WORKER_success",       # Namensdienst-Anfrage + worker_count
    ]
    for event in mandatory_events:
        assert event in src, f"Pflicht-Ereignis fehlt: {event}"
