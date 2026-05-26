"""
Tests für Issue #16: ReceiveResult — Ergebnis empfangen und speichern.
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


def _processing_task(task_id="t1"):
    t = Task(task_id=task_id, task_type="sum", payload="1,2")
    t.status = TaskState.PROCESSING
    t.timestamp_dispatched = 1_000_000
    return t


def _result_request(task_id="t1", success=True, result="3", error_msg="",
                    worker_id="w1", request_id="r1"):
    return taskgrid_pb2.ResultReturn(
        request_id=request_id,
        task_id=task_id,
        worker_id=worker_id,
        result=result,
        success=success,
        error_msg=error_msg,
    )


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_success_sets_completed():
    servicer, store = _make_servicer()
    task = _processing_task()
    store.add(task)

    ack = servicer.ReceiveResult(_result_request(result="3"), MagicMock())

    assert ack.ok is True
    updated = store.get("t1")
    assert updated.status == TaskState.COMPLETED
    assert updated.result == "3"
    assert updated.timestamp_completed > 0


def test_failure_sets_failed():
    servicer, store = _make_servicer()
    task = _processing_task()
    store.add(task)

    ack = servicer.ReceiveResult(
        _result_request(success=False, error_msg="worker_error"), MagicMock()
    )

    assert ack.ok is True
    updated = store.get("t1")
    assert updated.status == TaskState.FAILED
    assert updated.result == "worker_error"


def test_unknown_task_id_returns_nack():
    servicer, store = _make_servicer()

    ack = servicer.ReceiveResult(_result_request(task_id="unknown"), MagicMock())

    assert ack.ok is False
    assert "unknown" in ack.message


def test_already_completed_is_ignored():
    servicer, store = _make_servicer()
    task = _processing_task()
    task.status = TaskState.COMPLETED
    task.result = "original"
    store.add(task)

    ack = servicer.ReceiveResult(_result_request(result="overwrite"), MagicMock())

    assert ack.ok is True
    assert store.get("t1").result == "original"   # unverändert


def test_already_failed_is_ignored():
    servicer, store = _make_servicer()
    task = _processing_task()
    task.status = TaskState.FAILED
    store.add(task)

    ack = servicer.ReceiveResult(_result_request(), MagicMock())

    assert ack.ok is True
    updated = store.get("t1")
    assert updated.status == TaskState.FAILED


def test_cancel_timeout_called_on_success():
    dispatch_loop = MagicMock()
    servicer, store = _make_servicer(dispatch_loop=dispatch_loop)
    task = _processing_task()
    store.add(task)

    servicer.ReceiveResult(_result_request(), MagicMock())

    dispatch_loop.cancel_timeout.assert_called_once_with("t1")


def test_cancel_timeout_not_called_for_unknown_task():
    dispatch_loop = MagicMock()
    servicer, store = _make_servicer(dispatch_loop=dispatch_loop)

    servicer.ReceiveResult(_result_request(task_id="ghost"), MagicMock())

    dispatch_loop.cancel_timeout.assert_not_called()


def test_cancel_timeout_not_called_for_terminal_task():
    dispatch_loop = MagicMock()
    servicer, store = _make_servicer(dispatch_loop=dispatch_loop)
    task = _processing_task()
    task.status = TaskState.COMPLETED
    store.add(task)

    servicer.ReceiveResult(_result_request(), MagicMock())

    dispatch_loop.cancel_timeout.assert_not_called()


def test_multiple_results_only_first_counts():
    """Zweites RESULT_RETURN für denselben Task wird ignoriert."""
    servicer, store = _make_servicer()
    task = _processing_task()
    store.add(task)

    servicer.ReceiveResult(_result_request(result="first"), MagicMock())
    ack2 = servicer.ReceiveResult(_result_request(result="second"), MagicMock())

    assert ack2.ok is True
    assert store.get("t1").result == "first"
