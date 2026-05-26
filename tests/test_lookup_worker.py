"""
Tests für Issue #14: LOOKUP_WORKER — Worker dynamisch über Namensdienst auflösen.
Testet Servicer.lookup_worker und RoundRobinSelector direkt (kein laufender gRPC-Server).
"""

from unittest.mock import MagicMock, patch

from src.dispatcher.namensdienst_client import WorkerInfo
from src.dispatcher.servicer import DispatcherServicer
from src.dispatcher.task_queue import TaskQueue
from src.dispatcher.task_store import TaskStore
from src.dispatcher.worker_selector import RoundRobinSelector


def _make_servicer(workers_by_type: dict = None):
    """Erstellt Servicer mit gemocktem NamensdienstClient."""
    store = TaskStore()
    q = TaskQueue()
    ns_client = MagicMock()

    def _lookup(task_type, request_id=""):
        return (workers_by_type or {}).get(task_type, [])

    ns_client.lookup_worker.side_effect = _lookup
    servicer = DispatcherServicer(store, q, ns_client)
    return servicer, ns_client


# ── Erfolgsfälle ──────────────────────────────────────────────────────────────

def test_lookup_returns_worker_when_available():
    workers = {"sum": [WorkerInfo("w1", "worker-1", 60051, 0)]}
    servicer, _ = _make_servicer(workers)
    result = servicer.lookup_worker("sum", "req-1")
    assert result is not None
    assert result.worker_id == "w1"
    assert result.address == "worker-1"
    assert result.port == 60051


def test_lookup_returns_none_when_no_worker():
    servicer, _ = _make_servicer({})
    result = servicer.lookup_worker("sum", "req-2")
    assert result is None


def test_lookup_calls_namensdienst_with_correct_type():
    workers = {"hash": [WorkerInfo("w2", "worker-2", 60052, 1)]}
    servicer, ns_client = _make_servicer(workers)
    servicer.lookup_worker("hash", "req-3")
    ns_client.lookup_worker.assert_called_once_with("hash", "req-3")


def test_lookup_no_static_addresses():
    """Stellt sicher dass keine IP/Hostname im Dispatcher-Code steht."""
    import inspect
    import src.dispatcher.servicer as mod
    import src.dispatcher.server as srv_mod
    source = inspect.getsource(mod) + inspect.getsource(srv_mod)
    forbidden = ["192.168.", "10.0.", "172.", "localhost", "127.0.0.1",
                 "worker-1:60051", "worker:600"]
    for pattern in forbidden:
        assert pattern not in source, f"Statische Adresse gefunden: {pattern}"


# ── Round-Robin ───────────────────────────────────────────────────────────────

def test_round_robin_cycles_through_workers():
    selector = RoundRobinSelector()
    workers = [
        WorkerInfo("w1", "host1", 1, 0),
        WorkerInfo("w2", "host2", 2, 0),
        WorkerInfo("w3", "host3", 3, 0),
    ]
    selected = [selector.select("sum", workers).worker_id for _ in range(6)]
    assert selected == ["w1", "w2", "w3", "w1", "w2", "w3"]


def test_round_robin_independent_per_type():
    selector = RoundRobinSelector()
    w = [WorkerInfo("w1", "h", 1, 0), WorkerInfo("w2", "h", 2, 0)]
    selector.select("sum", w)   # sum → w1
    selector.select("sum", w)   # sum → w2
    # hash startet unabhängig bei w1
    assert selector.select("hash", w).worker_id == "w1"


def test_round_robin_returns_none_for_empty_list():
    selector = RoundRobinSelector()
    assert selector.select("sum", []) is None


def test_round_robin_single_worker_always_selected():
    selector = RoundRobinSelector()
    workers = [WorkerInfo("w1", "host1", 1, 0)]
    for _ in range(5):
        assert selector.select("sum", workers).worker_id == "w1"


# ── Fehlerfall: NS nicht erreichbar ───────────────────────────────────────────

def test_lookup_returns_none_when_namensdienst_unreachable():
    store = TaskStore()
    q = TaskQueue()
    ns_client = MagicMock()
    ns_client.lookup_worker.return_value = []   # NS gibt leere Liste zurück (Fehler intern)
    servicer = DispatcherServicer(store, q, ns_client)
    result = servicer.lookup_worker("reverse", "req-err")
    assert result is None
