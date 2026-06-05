"""
Tests für Issue #16: ReturnResult — Ergebnis empfangen und speichern.
"""

import pytest
from unittest.mock import MagicMock

from proto import taskgrid_pb2
from src.common.protocol import TaskState
from src.dispatcher.servicer import DispatcherServicer
from src.dispatcher.task import Task
from src.dispatcher.task_queue import TaskQueue
from src.dispatcher.task_store import TaskStore


# ── Hilfsfunktionen ──────────────────────────────────────────────────────────

def _make_servicer(dispatch_loop=None):
    store      = TaskStore()
    queue      = TaskQueue()
    ns_client  = MagicMock()
    servicer   = DispatcherServicer(store, queue, ns_client, dispatch_loop)
    return servicer, store


def _processing_task(task_id="1"):
    t = Task(task_id=task_id, task_type="sum", payload="1,2")
    t.status = TaskState.PROCESSING
    t.timestamp_dispatched = 1_000_000
    return t


def _result_request(task_id=1, status="COMPLETED", result="3", error="",
                    worker_id="w1", request_id="r1"):
    return taskgrid_pb2.ResultRequest(
        request_id=request_id,
        sender=worker_id,
        payload=taskgrid_pb2.ResultRequest.Payload(
            task_id=task_id,
            worker_id=worker_id,
            status=status,
            result=result,
            error=error,
        ),
    )


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_success_sets_completed():
    servicer, store = _make_servicer()
    task = _processing_task()
    store.add(task)

    ack = servicer.ReturnResult(_result_request(result="3"), MagicMock())

    assert ack.payload.success is True
    updated = store.get("1")
    assert updated.status == TaskState.COMPLETED
    assert updated.result == "3"
    assert updated.timestamp_completed > 0


def test_failure_sets_failed():
    servicer, store = _make_servicer()
    task = _processing_task()
    store.add(task)

    ack = servicer.ReturnResult(
        _result_request(status="FAILED", error="worker_error"), MagicMock()
    )

    assert ack.payload.success is True
    updated = store.get("1")
    assert updated.status == TaskState.FAILED
    assert updated.result == "worker_error"


def test_unknown_task_id_returns_nack():
    servicer, store = _make_servicer()

    ack = servicer.ReturnResult(_result_request(task_id=999), MagicMock())

    assert ack.payload.success is False
    assert "unknown" in ack.payload.message


def test_already_completed_is_ignored():
    servicer, store = _make_servicer()
    task = _processing_task()
    task.status = TaskState.COMPLETED
    task.result = "original"
    store.add(task)

    ack = servicer.ReturnResult(_result_request(result="overwrite"), MagicMock())

    assert ack.payload.success is True
    assert store.get("1").result == "original"   # unverändert


def test_already_failed_is_ignored():
    servicer, store = _make_servicer()
    task = _processing_task()
    task.status = TaskState.FAILED
    store.add(task)

    ack = servicer.ReturnResult(_result_request(), MagicMock())

    assert ack.payload.success is True
    updated = store.get("1")
    assert updated.status == TaskState.FAILED


def test_cancel_timeout_called_on_success():
    dispatch_loop = MagicMock()
    servicer, store = _make_servicer(dispatch_loop=dispatch_loop)
    task = _processing_task()
    store.add(task)

    servicer.ReturnResult(_result_request(), MagicMock())

    dispatch_loop.cancel_timeout.assert_called_once_with("1")


def test_cancel_timeout_not_called_for_unknown_task():
    dispatch_loop = MagicMock()
    servicer, store = _make_servicer(dispatch_loop=dispatch_loop)

    servicer.ReturnResult(_result_request(task_id=999), MagicMock())

    dispatch_loop.cancel_timeout.assert_not_called()


def test_cancel_timeout_not_called_for_terminal_task():
    dispatch_loop = MagicMock()
    servicer, store = _make_servicer(dispatch_loop=dispatch_loop)
    task = _processing_task()
    task.status = TaskState.COMPLETED
    store.add(task)

    servicer.ReturnResult(_result_request(), MagicMock())

    dispatch_loop.cancel_timeout.assert_not_called()


def test_multiple_results_only_first_counts():
    """Zweites ReturnResult für denselben Task wird ignoriert."""
    servicer, store = _make_servicer()
    task = _processing_task()
    store.add(task)

    servicer.ReturnResult(_result_request(result="first"), MagicMock())
    ack2 = servicer.ReturnResult(_result_request(result="second"), MagicMock())

    assert ack2.payload.success is True
    assert store.get("1").result == "first"
