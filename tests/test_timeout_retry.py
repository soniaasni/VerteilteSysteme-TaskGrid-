"""
Tests für Issue #18: Timeout-Handling und Retry-Logik im Dispatcher.
"""

import time
import threading
from unittest.mock import MagicMock

import pytest

from src.common.protocol import TaskState
from src.dispatcher.dispatch_loop import DispatchLoop
from src.dispatcher.namensdienst_client import WorkerInfo
from src.dispatcher.task import Task
from src.dispatcher.task_queue import TaskQueue
from src.dispatcher.task_store import TaskStore


# ── Hilfsfunktionen ──────────────────────────────────────────────────────────

def _dispatched_task(task_id="t1", retry_count=0):
    t = Task(task_id=task_id, task_type="sum", payload="1,2")
    t.status = TaskState.DISPATCHED
    t.timestamp_dispatched = int(time.time())
    t.assigned_worker = "w1"
    t.retry_count = retry_count
    return t


def _make_loop(workers_for_type=None, dispatch_ok=True,
               timeout_secs=0.1, max_retries=3):
    store         = TaskStore()
    q             = TaskQueue()
    ns_client     = MagicMock()
    worker_client = MagicMock()

    from src.dispatcher.worker_selector import RoundRobinSelector
    selector = RoundRobinSelector()

    def _lookup(task_type, request_id=""):
        return (workers_for_type or {}).get(task_type, [])

    ns_client.lookup_worker.side_effect = _lookup
    worker_client.dispatch_task.return_value = dispatch_ok

    loop = DispatchLoop(store, q, ns_client, selector, worker_client,
                        timeout_secs=timeout_secs, max_retries=max_retries)
    return loop, store, q


# ── Timeout-Zustandsübergänge ─────────────────────────────────────────────────

def test_timeout_sets_retrying_state():
    loop, store, q = _make_loop(timeout_secs=0.05, max_retries=3)
    task = _dispatched_task()
    store.add(task)

    loop._start_timeout_timer(task)
    time.sleep(0.2)

    updated = store.get("t1")
    assert updated.status == TaskState.RETRYING


def test_timeout_increments_retry_count():
    loop, store, q = _make_loop(timeout_secs=0.05, max_retries=3)
    task = _dispatched_task(retry_count=0)
    store.add(task)

    loop._start_timeout_timer(task)
    time.sleep(0.2)

    updated = store.get("t1")
    assert updated.retry_count == 1


def test_timeout_reenqueues_for_retry():
    loop, store, q = _make_loop(timeout_secs=0.05, max_retries=3)
    task = _dispatched_task(retry_count=0)
    store.add(task)

    loop._start_timeout_timer(task)
    time.sleep(0.2)

    assert q.size() == 1


def test_timeout_sets_failed_after_max_retries():
    loop, store, q = _make_loop(timeout_secs=0.05, max_retries=3)
    task = _dispatched_task(retry_count=2)   # next retry = 3 = MAX
    store.add(task)

    loop._start_timeout_timer(task)
    time.sleep(0.2)

    updated = store.get("t1")
    assert updated.status == TaskState.FAILED


def test_timeout_does_not_reenqueue_when_max_retries_exceeded():
    loop, store, q = _make_loop(timeout_secs=0.05, max_retries=3)
    task = _dispatched_task(retry_count=2)
    store.add(task)

    loop._start_timeout_timer(task)
    time.sleep(0.2)

    assert q.size() == 0


def test_timeout_ignored_if_task_already_completed():
    loop, store, q = _make_loop(timeout_secs=0.05, max_retries=3)
    task = _dispatched_task()
    task.status = TaskState.COMPLETED
    store.add(task)

    loop._start_timeout_timer(task)
    time.sleep(0.2)

    updated = store.get("t1")
    assert updated.status == TaskState.COMPLETED   # unverändert


def test_timeout_ignored_if_task_already_failed():
    loop, store, q = _make_loop(timeout_secs=0.05, max_retries=3)
    task = _dispatched_task()
    task.status = TaskState.FAILED
    store.add(task)

    loop._start_timeout_timer(task)
    time.sleep(0.2)

    assert q.size() == 0


# ── Retry-Dispatch ─────────────────────────────────────────────────────────────

def test_retrying_task_gets_dispatched():
    """Ein RETRYING-Task in der Queue wird erneut dispatched."""
    workers = {"sum": [WorkerInfo("w1", "worker-1", 60051, 0)]}
    loop, store, q = _make_loop(workers_for_type=workers, dispatch_ok=True,
                                timeout_secs=60, max_retries=3)

    task = _dispatched_task()
    task.status = TaskState.RETRYING
    task.retry_count = 1
    store.add(task)
    q.enqueue(task)

    loop.start()
    time.sleep(0.5)
    loop.stop()

    updated = store.get("t1")
    assert updated.status == TaskState.PROCESSING


# ── Retry-Zähler Ende-zu-Ende ──────────────────────────────────────────────────

def test_retry_count_accumulates_over_multiple_timeouts():
    """Zwei aufeinanderfolgende Timeouts erhöhen retry_count auf 2."""
    loop, store, q = _make_loop(timeout_secs=0.05, max_retries=5)
    task = _dispatched_task(retry_count=0)
    store.add(task)

    # Erster Timeout
    loop._start_timeout_timer(task)
    time.sleep(0.2)
    after_first = store.get("t1")
    assert after_first.retry_count == 1
    assert after_first.status == TaskState.RETRYING

    # Zweiten Timeout simulieren: Task auf DISPATCHED setzen
    after_first.status = TaskState.DISPATCHED
    store.update(after_first)

    loop._start_timeout_timer(after_first)
    time.sleep(0.2)
    after_second = store.get("t1")
    assert after_second.retry_count == 2


def test_max_retries_env_default_is_3():
    from src.dispatcher.dispatch_loop import _DEFAULT_MAX_RETRIES
    assert _DEFAULT_MAX_RETRIES == 3
