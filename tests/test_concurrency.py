"""
Tests für Issue #21: Queue-Verwaltung und Nebenläufigkeit.
Beweist Thread-Sicherheit aller zentralen Datenstrukturen unter Last.
"""

import threading
import time
from unittest.mock import MagicMock

import pytest

from src.common.protocol import TaskState, new_task_id
from src.dispatcher.namensdienst_client import WorkerInfo
from src.dispatcher.task import Task
from src.dispatcher.task_queue import TaskQueue
from src.dispatcher.task_store import TaskStore
from src.dispatcher.worker_selector import RoundRobinSelector


# ── Hilfsfunktionen ──────────────────────────────────────────────────────────

def _task(task_id=None, task_type="sum", status=TaskState.QUEUED):
    t = Task(task_id=task_id or new_task_id(), task_type=task_type, payload="x")
    t.status = status
    return t


def _workers(n=3):
    return [WorkerInfo(worker_id=f"w{i}", address="h", port=i, current_load=0)
            for i in range(n)]


# ── TaskQueue Thread-Sicherheit ───────────────────────────────────────────────

def test_queue_concurrent_enqueue_dequeue_no_loss():
    """N Threads enqueueing und N Threads dequeueing — kein Task geht verloren."""
    q = TaskQueue()
    n = 100
    enqueued = []
    dequeued = []
    lock = threading.Lock()

    def producer():
        for _ in range(10):
            t = _task()
            with lock:
                enqueued.append(t.task_id)
            q.enqueue(t)

    def consumer():
        collected = []
        deadline = time.time() + 5.0
        while time.time() < deadline and len(collected) < 10:
            t = q.dequeue(timeout=0.1)
            if t:
                collected.append(t.task_id)
        with lock:
            dequeued.extend(collected)

    threads = [threading.Thread(target=producer) for _ in range(10)]
    threads += [threading.Thread(target=consumer) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert len(enqueued) == 100
    assert len(dequeued) == 100
    assert sorted(enqueued) == sorted(dequeued)


def test_queue_size_consistent_under_concurrent_access():
    """queue.size() gibt unter parallelem Zugriff keinen negativen Wert zurück."""
    q = TaskQueue()
    for _ in range(50):
        q.enqueue(_task())

    sizes = []
    lock = threading.Lock()

    def read_size():
        for _ in range(20):
            with lock:
                sizes.append(q.size())

    threads = [threading.Thread(target=read_size) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert all(s >= 0 for s in sizes)


# ── TaskStore Thread-Sicherheit ───────────────────────────────────────────────

def test_store_concurrent_add_no_duplicate_ids():
    """Gleichzeitiges Hinzufügen von Tasks mit new_task_id() erzeugt keine Duplikate."""
    store = TaskStore()
    errors = []

    def add_tasks():
        for _ in range(50):
            t = _task()
            try:
                store.add(t)
            except Exception as e:
                errors.append(str(e))

    threads = [threading.Thread(target=add_tasks) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    ids = [t.task_id for t in store.all_tasks()]
    assert len(ids) == len(set(ids)), "Duplizierte Task-IDs gefunden"
    assert len(ids) == 500


def test_store_concurrent_get_update_no_exception():
    """Gleichzeitiges Lesen und Schreiben auf denselben Task wirft keine Exception."""
    store = TaskStore()
    task = _task(task_id="shared")
    store.add(task)
    errors = []

    def reader():
        for _ in range(100):
            t = store.get("shared")
            assert t is not None

    def writer():
        for i in range(100):
            t = store.get("shared")
            t.retry_count = i
            try:
                store.update(t)
            except Exception as e:
                errors.append(str(e))

    threads = [threading.Thread(target=reader) for _ in range(5)]
    threads += [threading.Thread(target=writer) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []


def test_store_all_tasks_snapshot_is_consistent():
    """all_tasks() gibt einen konsistenten Snapshot zurück, kein partiell-befülltes Resultat."""
    store = TaskStore()
    stop = threading.Event()
    snapshots = []
    lock = threading.Lock()

    def adder():
        for i in range(200):
            store.add(_task(task_id=f"a{i}"))

    def reader():
        while not stop.is_set():
            snap = store.all_tasks()
            with lock:
                snapshots.append(len(snap))

    adder_thread = threading.Thread(target=adder)
    reader_thread = threading.Thread(target=reader)

    reader_thread.start()
    adder_thread.start()
    adder_thread.join()
    stop.set()
    reader_thread.join()

    # Kein Snapshot darf größer als 200 sein
    assert all(0 <= s <= 200 for s in snapshots)


def test_store_worker_load_map_under_concurrent_updates():
    """worker_load_map() gibt unter parallelen Updates konsistente Werte zurück."""
    store = TaskStore()
    errors = []

    def add_and_update():
        for i in range(20):
            t = _task(task_id=new_task_id(), status=TaskState.DISPATCHED)
            t.assigned_worker = f"w{i % 3}"
            store.add(t)
            try:
                _ = store.worker_load_map()
            except Exception as e:
                errors.append(str(e))

    threads = [threading.Thread(target=add_and_update) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    load = store.worker_load_map()
    assert sum(load.values()) == 100   # 5 threads × 20 tasks


# ── RoundRobinSelector Thread-Sicherheit ─────────────────────────────────────

def test_round_robin_no_index_out_of_bounds_under_concurrency():
    """Gleichzeitige select()-Aufrufe dürfen keinen IndexError auslösen."""
    selector = RoundRobinSelector()
    workers = _workers(3)
    errors = []

    def select_many():
        for _ in range(200):
            try:
                w = selector.select("sum", workers)
                assert w in workers
            except Exception as e:
                errors.append(str(e))

    threads = [threading.Thread(target=select_many) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []


def test_round_robin_distributes_evenly_under_concurrency():
    """Unter parallelem Zugriff bleibt die Verteilung annähernd gleichmäßig."""
    selector = RoundRobinSelector()
    workers = _workers(3)
    counts = {w.worker_id: 0 for w in workers}
    lock = threading.Lock()

    def select_many():
        for _ in range(100):
            w = selector.select("sum", workers)
            with lock:
                counts[w.worker_id] += 1

    threads = [threading.Thread(target=select_many) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    total = sum(counts.values())
    assert total == 1000
    # Jeder Worker bekommt zwischen 25% und 42% (idealer Wert: 33%)
    for wid, cnt in counts.items():
        assert 250 <= cnt <= 420, f"Worker {wid} hat {cnt}/1000 Tasks — ungleichmäßig"


# ── Dispatcher Servicer Nebenläufigkeit ───────────────────────────────────────

def test_concurrent_post_task_all_stored():
    """100 gleichzeitige PostTask-Aufrufe → alle 100 Tasks landen im Store."""
    from proto import taskgrid_pb2
    from src.dispatcher.servicer import DispatcherServicer
    from src.dispatcher.task_queue import TaskQueue

    store = TaskStore()
    q = TaskQueue()
    ns = MagicMock()
    ns.lookup_worker.return_value = []
    servicer = DispatcherServicer(store, q, ns)
    errors = []

    def post():
        req = taskgrid_pb2.PostTaskRequest(
            request_id="r", task_type="sum", task_payload="1,2", sender="c"
        )
        ctx = MagicMock()
        try:
            servicer.PostTask(req, ctx)
        except Exception as e:
            errors.append(str(e))

    threads = [threading.Thread(target=post) for _ in range(100)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    assert len(store.all_tasks()) == 100
    assert q.size() == 100


def test_concurrent_return_result_idempotent():
    """Mehrere gleichzeitige ReturnResult-Aufrufe auf denselben Task → nur erste zählt."""
    from proto import taskgrid_pb2
    from src.dispatcher.servicer import DispatcherServicer
    from src.dispatcher.task_queue import TaskQueue

    store = TaskStore()
    task = _task(task_id="42", status=TaskState.PROCESSING)
    task.timestamp_dispatched = int(time.time()) - 1
    store.add(task)

    q = TaskQueue()
    ns = MagicMock()
    servicer = DispatcherServicer(store, q, ns)
    results = []
    lock = threading.Lock()

    def return_result(result_val):
        req = taskgrid_pb2.ResultRequest(
            request_id="r", task_id=42, worker_id="w1",
            status="COMPLETED", result=result_val,
        )
        ack = servicer.ReturnResult(req, MagicMock())
        with lock:
            results.append(ack.success)

    threads = [threading.Thread(target=return_result, args=(f"val{i}",))
               for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Alle Acks müssen success=True sein (Idempotenz)
    assert all(results)
    # Task-Zustand ist COMPLETED
    assert store.get("42").status == TaskState.COMPLETED


# ── worker_load_map Korrektheit ───────────────────────────────────────────────

def test_worker_load_map_counts_dispatched_and_processing():
    store = TaskStore()
    for i, status in enumerate([TaskState.DISPATCHED, TaskState.PROCESSING,
                                  TaskState.COMPLETED, TaskState.QUEUED]):
        t = _task(task_id=str(i), status=status)
        t.assigned_worker = "w1"
        store.add(t)

    load = store.worker_load_map()
    assert load.get("w1", 0) == 2   # nur DISPATCHED + PROCESSING


def test_worker_load_map_empty_when_no_active_tasks():
    store = TaskStore()
    t = _task(task_id="1", status=TaskState.COMPLETED)
    t.assigned_worker = "w1"
    store.add(t)

    load = store.worker_load_map()
    assert load == {}


def test_worker_load_map_multiple_workers():
    store = TaskStore()
    for i in range(6):
        t = _task(task_id=str(i), status=TaskState.PROCESSING)
        t.assigned_worker = f"w{i % 3}"
        store.add(t)

    load = store.worker_load_map()
    assert load == {"w0": 2, "w1": 2, "w2": 2}
