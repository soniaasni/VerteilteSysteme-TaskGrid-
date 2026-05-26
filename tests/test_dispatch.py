"""
Tests für Issue #15: dispatch_task_to_worker — Zustandsübergänge und Dispatch-Loop.
"""

import time
import threading
from unittest.mock import MagicMock, patch

from src.common.protocol import TaskState
from src.dispatcher.dispatch_loop import DispatchLoop
from src.dispatcher.namensdienst_client import WorkerInfo
from src.dispatcher.state_machine import (
    VALID_TRANSITIONS, InvalidTransitionError, is_terminal, transition,
)
from src.dispatcher.task import Task
from src.dispatcher.task_queue import TaskQueue
from src.dispatcher.task_store import TaskStore
from src.dispatcher.worker_client import WorkerClient
from src.dispatcher.worker_selector import RoundRobinSelector


# ── Hilfsfunktionen ──────────────────────────────────────────────────────────

def _queued_task(task_id="t1", task_type="sum", payload="1,2"):
    t = Task(task_id=task_id, task_type=task_type, payload=payload)
    t.status = TaskState.QUEUED
    return t


def _make_loop(workers_for_type=None, dispatch_ok=True, timeout_secs=30):
    store         = TaskStore()
    q             = TaskQueue()
    ns_client     = MagicMock()
    selector      = RoundRobinSelector()
    worker_client = MagicMock()

    def _lookup(task_type, request_id=""):
        return (workers_for_type or {}).get(task_type, [])

    ns_client.lookup_worker.side_effect = _lookup
    worker_client.dispatch_task.return_value = dispatch_ok

    loop = DispatchLoop(store, q, ns_client, selector, worker_client,
                        timeout_secs=timeout_secs)
    return loop, store, q, ns_client, worker_client


# ── State Machine Tests ───────────────────────────────────────────────────────

def test_valid_transition_queued_to_dispatched():
    task = _queued_task()
    transition(task, TaskState.DISPATCHED)
    assert task.status == TaskState.DISPATCHED
    assert task.timestamp_dispatched > 0


def test_valid_transition_dispatched_to_processing():
    task = _queued_task()
    transition(task, TaskState.DISPATCHED)
    transition(task, TaskState.PROCESSING)
    assert task.status == TaskState.PROCESSING


def test_valid_transition_processing_to_completed():
    task = _queued_task()
    transition(task, TaskState.DISPATCHED)
    transition(task, TaskState.PROCESSING)
    transition(task, TaskState.COMPLETED)
    assert task.status == TaskState.COMPLETED
    assert task.timestamp_completed > 0


def test_invalid_transition_completed_to_dispatched():
    task = _queued_task()
    transition(task, TaskState.DISPATCHED)
    transition(task, TaskState.PROCESSING)
    transition(task, TaskState.COMPLETED)
    with pytest.raises(InvalidTransitionError):
        transition(task, TaskState.DISPATCHED)


def test_invalid_transition_failed_to_dispatched():
    task = _queued_task()
    transition(task, TaskState.FAILED)
    with pytest.raises(InvalidTransitionError):
        transition(task, TaskState.DISPATCHED)


def test_terminal_states_are_final():
    for terminal in (TaskState.COMPLETED, TaskState.FAILED):
        task = _queued_task()
        task.status = terminal
        assert is_terminal(task)
        assert VALID_TRANSITIONS[terminal] == set()


def test_all_8_states_defined_in_transitions():
    assert set(VALID_TRANSITIONS.keys()) == set(TaskState)


# ── Dispatch-Loop Tests ───────────────────────────────────────────────────────

import pytest

def test_dispatch_sets_status_processing_on_success():
    workers = {"sum": [WorkerInfo("w1", "worker-1", 60051, 0)]}
    loop, store, q, _, _ = _make_loop(workers_for_type=workers, dispatch_ok=True)

    task = _queued_task()
    store.add(task)
    q.enqueue(task)

    loop.start()
    time.sleep(0.5)
    loop.stop()

    updated = store.get("t1")
    assert updated.status == TaskState.PROCESSING
    assert updated.assigned_worker == "w1"
    assert updated.timestamp_dispatched > 0


def test_dispatch_sets_status_failed_when_worker_unreachable():
    workers = {"sum": [WorkerInfo("w1", "worker-1", 60051, 0)]}
    loop, store, q, _, _ = _make_loop(workers_for_type=workers, dispatch_ok=False)

    task = _queued_task()
    store.add(task)
    q.enqueue(task)

    loop.start()
    time.sleep(0.5)
    loop.stop()

    updated = store.get("t1")
    assert updated.status == TaskState.FAILED


def test_dispatch_reenqueues_when_no_worker(monkeypatch):
    monkeypatch.setattr("src.dispatcher.dispatch_loop._NO_WORKER_RETRY_SECS", 0.0)
    loop, store, q, _, _ = _make_loop(workers_for_type={}, dispatch_ok=True)

    task = _queued_task()
    store.add(task)
    q.enqueue(task)

    loop.start()
    time.sleep(0.3)
    loop.stop()

    # Task muss noch QUEUED sein — wurde re-enqueued
    updated = store.get("t1")
    assert updated.status == TaskState.QUEUED


def test_dispatch_starts_timeout_timer():
    workers = {"sum": [WorkerInfo("w1", "worker-1", 60051, 0)]}
    loop, store, q, _, wc = _make_loop(workers_for_type=workers, dispatch_ok=True,
                                        timeout_secs=60)

    task = _queued_task()
    store.add(task)
    q.enqueue(task)

    loop.start()
    time.sleep(0.5)
    loop.stop()

    # Timer läuft weiter bis ReturnResult eintrifft — nicht bei Worker-Akzeptanz gecancelt
    with loop._timers_lock:
        assert "t1" in loop._timers, "Timeout-Timer muss nach Akzeptanz noch laufen"


def test_cancel_timeout_removes_timer():
    loop, store, q, _, _ = _make_loop()
    dummy_timer = MagicMock()
    with loop._timers_lock:
        loop._timers["t99"] = dummy_timer

    loop.cancel_timeout("t99")

    dummy_timer.cancel.assert_called_once()
    with loop._timers_lock:
        assert "t99" not in loop._timers


def test_parallel_dispatch_multiple_tasks():
    """Mehrere Tasks müssen gleichzeitig dispatched werden können."""
    workers = {"sum": [WorkerInfo("w1", "worker-1", 60051, 0),
                       WorkerInfo("w2", "worker-2", 60052, 0)]}
    loop, store, q, _, _ = _make_loop(workers_for_type=workers, dispatch_ok=True)

    tasks = [_queued_task(task_id=f"t{i}") for i in range(5)]
    for t in tasks:
        store.add(t)
        q.enqueue(t)

    loop.start()
    time.sleep(1.0)
    loop.stop()

    for t in tasks:
        updated = store.get(t.task_id)
        assert updated.status == TaskState.PROCESSING
