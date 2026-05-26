"""
Tests für Issue #17: GetResult — Ergebnis an Client zurückgeben.
"""

import grpc
import pytest
from unittest.mock import MagicMock

from proto import taskgrid_pb2
from src.common.protocol import TaskState
from src.dispatcher.servicer import DispatcherServicer
from src.dispatcher.task import Task
from src.dispatcher.task_queue import TaskQueue
from src.dispatcher.task_store import TaskStore


# ── Hilfsfunktionen ──────────────────────────────────────────────────────────

def _make_servicer():
    store     = TaskStore()
    queue     = TaskQueue()
    ns_client = MagicMock()
    servicer  = DispatcherServicer(store, queue, ns_client)
    return servicer, store


def _result_request(task_id="t1", sender="client-1", request_id="r1"):
    return taskgrid_pb2.ResultRequest(
        request_id=request_id,
        task_id=task_id,
        sender=sender,
    )


def _task(task_id="t1", status=TaskState.COMPLETED, result=""):
    t = Task(task_id=task_id, task_type="sum", payload="1,2")
    t.status = status
    t.result = result
    return t


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_completed_returns_result_and_status():
    servicer, store = _make_servicer()
    store.add(_task(status=TaskState.COMPLETED, result="42"))

    ctx = MagicMock()
    resp = servicer.GetResult(_result_request(), ctx)

    assert resp.task_id == "t1"
    assert resp.status == TaskState.COMPLETED.value
    assert resp.result == "42"
    ctx.set_code.assert_not_called()


def test_failed_returns_error_in_result():
    servicer, store = _make_servicer()
    store.add(_task(status=TaskState.FAILED, result="worker_crashed"))

    ctx = MagicMock()
    resp = servicer.GetResult(_result_request(), ctx)

    assert resp.status == TaskState.FAILED.value
    assert resp.result == "worker_crashed"


def test_queued_returns_status_empty_result():
    servicer, store = _make_servicer()
    store.add(_task(status=TaskState.QUEUED))

    ctx = MagicMock()
    resp = servicer.GetResult(_result_request(), ctx)

    assert resp.status == TaskState.QUEUED.value
    assert resp.result == ""
    ctx.set_code.assert_not_called()


def test_processing_returns_status_empty_result():
    servicer, store = _make_servicer()
    store.add(_task(status=TaskState.PROCESSING))

    ctx = MagicMock()
    resp = servicer.GetResult(_result_request(), ctx)

    assert resp.status == TaskState.PROCESSING.value
    assert resp.result == ""


def test_dispatched_returns_status_empty_result():
    servicer, store = _make_servicer()
    store.add(_task(status=TaskState.DISPATCHED))

    ctx = MagicMock()
    resp = servicer.GetResult(_result_request(), ctx)

    assert resp.status == TaskState.DISPATCHED.value
    assert resp.result == ""


def test_unknown_task_id_returns_not_found():
    servicer, store = _make_servicer()

    ctx = MagicMock()
    resp = servicer.GetResult(_result_request(task_id="ghost"), ctx)

    ctx.set_code.assert_called_once_with(grpc.StatusCode.NOT_FOUND)
    assert resp.status == "NOT_FOUND"


def test_unknown_task_id_sets_details():
    servicer, store = _make_servicer()

    ctx = MagicMock()
    servicer.GetResult(_result_request(task_id="missing"), ctx)

    ctx.set_details.assert_called_once()
    assert "missing" in ctx.set_details.call_args[0][0]


def test_task_id_in_response():
    servicer, store = _make_servicer()
    store.add(_task(task_id="abc123", status=TaskState.COMPLETED, result="ok"))

    ctx = MagicMock()
    resp = servicer.GetResult(_result_request(task_id="abc123"), ctx)

    assert resp.task_id == "abc123"
