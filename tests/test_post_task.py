"""
Tests für Issue #13: POST_TASK — Task entgegennehmen, ID vergeben, Queue.
Testet den Servicer direkt (ohne laufenden gRPC-Server).
"""

import pytest
from unittest.mock import MagicMock

from proto import taskgrid_pb2
from src.common.protocol import TaskState
from src.dispatcher.servicer import DispatcherServicer
from src.dispatcher.task_store import TaskStore
from src.dispatcher.task_queue import TaskQueue


def _make_servicer():
    from unittest.mock import MagicMock
    store = TaskStore()
    q = TaskQueue()
    ns_client = MagicMock()
    ns_client.lookup_worker.return_value = []
    return DispatcherServicer(store, q, ns_client), store, q


def _mock_context():
    ctx = MagicMock()
    ctx.set_code = MagicMock()
    ctx.set_details = MagicMock()
    return ctx


# ── Erfolgsfälle ──────────────────────────────────────────────────────────────

def test_post_task_returns_task_id():
    servicer, store, q = _make_servicer()
    req = taskgrid_pb2.PostTaskRequest(
        request_id="req-001",
        task_type="sum",
        task_payload="1,2,3,4",
        sender="client-1",
    )
    resp = servicer.PostTask(req, _mock_context())
    assert resp.task_id != 0
    assert resp.status == TaskState.QUEUED.value


def test_post_task_stores_task():
    servicer, store, q = _make_servicer()
    req = taskgrid_pb2.PostTaskRequest(
        request_id="req-002",
        task_type="reverse",
        task_payload="hello",
        sender="client-1",
    )
    resp = servicer.PostTask(req, _mock_context())
    task = store.get(str(resp.task_id))
    assert task is not None
    assert task.task_type == "reverse"
    assert task.payload == "hello"


def test_post_task_enqueues_task():
    servicer, store, q = _make_servicer()
    assert q.size() == 0
    req = taskgrid_pb2.PostTaskRequest(
        request_id="req-003",
        task_type="hash",
        task_payload="test",
        sender="client-1",
    )
    servicer.PostTask(req, _mock_context())
    assert q.size() == 1


def test_post_task_state_is_queued():
    """Task muss nach PostTask den Zustand QUEUED haben, nicht CREATED."""
    servicer, store, q = _make_servicer()
    req = taskgrid_pb2.PostTaskRequest(
        request_id="req-004",
        task_type="upper",
        task_payload="hello",
        sender="client-1",
    )
    resp = servicer.PostTask(req, _mock_context())
    task = store.get(str(resp.task_id))
    assert task.status == TaskState.QUEUED


def test_post_task_unique_ids():
    """Jeder Task muss eine einzigartige ID erhalten."""
    servicer, store, q = _make_servicer()
    ids = set()
    for i in range(10):
        req = taskgrid_pb2.PostTaskRequest(
            request_id=f"req-{i}",
            task_type="sum",
            task_payload=str(i),
            sender="client-1",
        )
        resp = servicer.PostTask(req, _mock_context())
        ids.add(resp.task_id)
    assert len(ids) == 10


# ── Fehlerfälle ───────────────────────────────────────────────────────────────

def test_post_task_rejects_empty_type():
    import grpc
    servicer, _, _ = _make_servicer()
    ctx = _mock_context()
    req = taskgrid_pb2.PostTaskRequest(
        request_id="req-err-1",
        task_type="",
        task_payload="data",
        sender="client-1",
    )
    servicer.PostTask(req, ctx)
    ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)


def test_post_task_rejects_type_too_long():
    import grpc
    servicer, _, _ = _make_servicer()
    ctx = _mock_context()
    req = taskgrid_pb2.PostTaskRequest(
        request_id="req-err-2",
        task_type="x" * 33,   # max ist 32
        task_payload="data",
        sender="client-1",
    )
    servicer.PostTask(req, ctx)
    ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)


def test_post_task_rejects_payload_too_large():
    import grpc
    servicer, _, _ = _make_servicer()
    ctx = _mock_context()
    req = taskgrid_pb2.PostTaskRequest(
        request_id="req-err-3",
        task_type="sum",
        task_payload="x" * 1025,   # max ist 1024
        sender="client-1",
    )
    servicer.PostTask(req, ctx)
    ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)


def test_post_task_does_not_store_on_error():
    """Abgelehnte Tasks dürfen nicht im Store landen."""
    servicer, store, q = _make_servicer()
    req = taskgrid_pb2.PostTaskRequest(
        request_id="req-err-4",
        task_type="",
        task_payload="data",
        sender="client-1",
    )
    servicer.PostTask(req, _mock_context())
    assert len(store.all_tasks()) == 0
    assert q.size() == 0
