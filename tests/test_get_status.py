"""
Tests für Issue #20: GET_STATUS — Monitoring-Schnittstelle.
"""

import json
import time
from unittest.mock import MagicMock

import pytest

from src.common.protocol import TaskState
from src.dispatcher.namensdienst_client import WorkerInfo
from src.dispatcher.status_collector import StatusCollector
from src.dispatcher.task import Task
from src.dispatcher.task_store import TaskStore


# ── Hilfsfunktionen ──────────────────────────────────────────────────────────

def _store_with(*tasks):
    store = TaskStore()
    for t in tasks:
        store.add(t)
    return store


def _task(task_id="1", task_type="sum", status=TaskState.COMPLETED,
          dispatched=0, completed=0, retries=0):
    t = Task(task_id=task_id, task_type=task_type, payload="x")
    t.status = status
    t.timestamp_dispatched = dispatched
    t.timestamp_completed = completed
    t.retry_count = retries
    return t


def _ns_with_workers(*worker_ids):
    ns = MagicMock()
    workers = [WorkerInfo(worker_id=w, address="localhost", port=50051, current_load=0)
               for w in worker_ids]
    ns.lookup_worker.return_value = workers
    return ns


# ── Tests StatusCollector ─────────────────────────────────────────────────────

def test_empty_store_returns_zeros():
    collector = StatusCollector(TaskStore(), _ns_with_workers())
    status = collector.get_status()

    assert status["offene_tasks"] == 0
    assert status["laufende_tasks"] == 0
    assert status["abgeschlossene_tasks"] == 0
    assert status["fehlgeschlagene_tasks"] == 0
    assert status["aktive_worker"] == 0
    assert status["unterstützte_tasktypen"] == []
    assert status["durchschnittliche_bearbeitungszeit_ms"] == 0.0
    assert status["anzahl_timeouts"] == 0
    assert status["anzahl_retries"] == 0


def test_counts_by_status():
    store = _store_with(
        _task("1", status=TaskState.QUEUED),
        _task("2", status=TaskState.QUEUED),
        _task("3", status=TaskState.PROCESSING),
        _task("4", status=TaskState.COMPLETED),
        _task("5", status=TaskState.FAILED),
    )
    collector = StatusCollector(store, _ns_with_workers())
    s = collector.get_status()

    assert s["offene_tasks"] == 2
    assert s["laufende_tasks"] == 1
    assert s["abgeschlossene_tasks"] == 1
    assert s["fehlgeschlagene_tasks"] == 1


def test_dispatched_and_retrying_count_as_running():
    store = _store_with(
        _task("1", status=TaskState.DISPATCHED),
        _task("2", status=TaskState.RETRYING),
    )
    collector = StatusCollector(store, _ns_with_workers())
    s = collector.get_status()
    assert s["laufende_tasks"] == 2


def test_avg_processing_time_calculated():
    store = _store_with(
        _task("1", status=TaskState.COMPLETED, dispatched=1000, completed=1001),
        _task("2", status=TaskState.COMPLETED, dispatched=1000, completed=1002),
    )
    collector = StatusCollector(store, _ns_with_workers())
    s = collector.get_status()
    assert s["durchschnittliche_bearbeitungszeit_ms"] == 1500.0


def test_avg_time_ignores_tasks_without_timestamps():
    store = _store_with(
        _task("1", status=TaskState.COMPLETED, dispatched=0, completed=0),
        _task("2", status=TaskState.COMPLETED, dispatched=1000, completed=1001),
    )
    collector = StatusCollector(store, _ns_with_workers())
    s = collector.get_status()
    assert s["durchschnittliche_bearbeitungszeit_ms"] == 1000.0


def test_task_types_collected():
    store = _store_with(
        _task("1", task_type="sum"),
        _task("2", task_type="reverse"),
        _task("3", task_type="sum"),
    )
    collector = StatusCollector(store, _ns_with_workers())
    s = collector.get_status()
    assert set(s["unterstützte_tasktypen"]) == {"sum", "reverse"}


def test_active_workers_deduplicated():
    store = _store_with(
        _task("1", task_type="sum"),
        _task("2", task_type="reverse"),
    )
    ns = MagicMock()
    ns.lookup_worker.side_effect = lambda tt, *a: [
        WorkerInfo(worker_id="w1", address="h", port=1, current_load=0),
        WorkerInfo(worker_id="w2", address="h", port=1, current_load=0),
    ]
    collector = StatusCollector(store, ns)
    s = collector.get_status()
    assert s["aktive_worker"] == 2


def test_timeout_and_retry_counts_from_retry_count():
    store = _store_with(
        _task("1", retries=2),
        _task("2", retries=1),
        _task("3", retries=0),
    )
    collector = StatusCollector(store, _ns_with_workers())
    s = collector.get_status()
    assert s["anzahl_timeouts"] == 3
    assert s["anzahl_retries"] == 3


def test_all_required_fields_present():
    collector = StatusCollector(TaskStore(), _ns_with_workers())
    s = collector.get_status()
    required = {
        "aktive_worker", "unterstützte_tasktypen", "offene_tasks",
        "laufende_tasks", "abgeschlossene_tasks", "fehlgeschlagene_tasks",
        "durchschnittliche_bearbeitungszeit_ms", "anzahl_timeouts", "anzahl_retries",
    }
    assert required <= s.keys()


# ── Tests GetStatus gRPC ──────────────────────────────────────────────────────

def test_grpc_get_status_returns_counts():
    from proto import taskgrid_pb2
    from src.dispatcher.servicer import DispatcherServicer
    from src.dispatcher.task_queue import TaskQueue

    store = _store_with(
        _task("1", status=TaskState.QUEUED),
        _task("2", status=TaskState.COMPLETED),
    )
    ns = _ns_with_workers()
    collector = StatusCollector(store, ns)
    servicer = DispatcherServicer(store, TaskQueue(), ns, collector=collector)

    req = taskgrid_pb2.StatusRequest(sender="monitor")
    ctx = MagicMock()
    resp = servicer.GetStatus(req, ctx)

    assert resp.queued_tasks >= 1
    ctx.set_code.assert_not_called()


def test_grpc_get_status_details_is_valid_json():
    from proto import taskgrid_pb2
    from src.dispatcher.servicer import DispatcherServicer
    from src.dispatcher.task_queue import TaskQueue

    store = _store_with(_task("1", status=TaskState.COMPLETED))
    ns = _ns_with_workers()
    collector = StatusCollector(store, ns)
    servicer = DispatcherServicer(store, TaskQueue(), ns, collector=collector)

    req = taskgrid_pb2.StatusRequest(sender="monitor")
    resp = servicer.GetStatus(req, MagicMock())

    parsed = json.loads(resp.details)
    assert "abgeschlossene_tasks" in parsed
