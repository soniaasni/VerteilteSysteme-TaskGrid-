"""
Phase-2 Integrationstest -- startet echten gRPC-Server und HTTP-Status-Endpoint,
testet alle Dispatcher-Funktionen live ohne Mocks (kein Docker noetig).

Getestete Issues:
  #13 POST_TASK
  #14 lookup_worker / RoundRobin
  #16 ReturnResult
  #17 GetResult
  #18 Timeout + Retry
  #19 Strukturiertes Logging
  #20 GET_STATUS (gRPC + HTTP)
  #21 Nebenlaeuigkeit

Ausfuehren:
  pytest tests/test_integration.py -v
"""

import json
import os
import threading
import time
import urllib.request
from concurrent import futures
from unittest.mock import MagicMock

import grpc
import pytest

from proto import taskgrid_pb2, taskgrid_pb2_grpc
from src.common.protocol import TaskState
from src.dispatcher.dispatch_loop import DispatchLoop
from src.dispatcher.http_status_server import HttpStatusServer
from src.dispatcher.namensdienst_client import NamensdienstClient, WorkerInfo
from src.dispatcher.servicer import DispatcherServicer
from src.dispatcher.status_collector import StatusCollector
from src.dispatcher.task_queue import TaskQueue
from src.dispatcher.task_store import TaskStore
from src.dispatcher.worker_client import WorkerClient
from src.dispatcher.worker_selector import RoundRobinSelector

# Ports abseits von Standard um Konflikte zu vermeiden
GRPC_PORT = 59051
HTTP_PORT = 59080


# ── Hilfsfunktionen ───────────────────────────────────────────────────────────

def _make_ns_client_mock(workers=None):
    mock = MagicMock(spec=NamensdienstClient)
    mock.lookup_worker.return_value = workers or []
    return mock


def _make_worker_client_mock(accept=True):
    mock = MagicMock(spec=WorkerClient)
    mock.dispatch_task.return_value = accept
    return mock


def _post_task(stub, task_type, task_payload, request_id="req", sender="test"):
    """PostTask mit korrekter Payload-Struktur."""
    return stub.PostTask(taskgrid_pb2.PostTaskRequest(
        message_type="POST_TASK",
        request_id=request_id,
        timestamp=int(time.time()),
        sender=sender,
        payload=taskgrid_pb2.PostTaskRequest.Payload(
            task_type=task_type,
            task_payload=task_payload,
        ),
    ))


def _return_result(stub, task_id, worker_id, status, result="", error=""):
    """ReturnResult mit korrekter Payload-Struktur."""
    return stub.ReturnResult(taskgrid_pb2.ResultRequest(
        message_type="RETURN_RESULT",
        request_id=f"rr-{task_id}",
        timestamp=int(time.time()),
        sender=worker_id,
        payload=taskgrid_pb2.ResultRequest.Payload(
            task_id=task_id,
            worker_id=worker_id,
            status=status,
            result=result,
            error=error,
        ),
    ))


def _get_result(stub, task_id, sender="test"):
    """GetResult mit korrekter Payload-Struktur."""
    return stub.GetResult(taskgrid_pb2.GetResultRequest(
        message_type="GET_RESULT",
        request_id=f"gr-{task_id}",
        timestamp=int(time.time()),
        sender=sender,
        payload=taskgrid_pb2.GetResultRequest.Payload(
            task_id=task_id,
        ),
    ))


def _wait_for_dispatch(store, task_id, timeout=3.0):
    """Wartet bis Task DISPATCHED oder PROCESSING ist."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        t = store.get(str(task_id))
        if t and t.status in (TaskState.PROCESSING, TaskState.DISPATCHED):
            return t
        time.sleep(0.05)
    return store.get(str(task_id))


def _wait_for_processing(store, task_id, timeout=3.0):
    """Wartet bis Task PROCESSING ist."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        t = store.get(str(task_id))
        if t and t.status == TaskState.PROCESSING:
            return t
        time.sleep(0.05)
    return store.get(str(task_id))


# ── Shared-Server-Fixture (scope=module) ─────────────────────────────────────

@pytest.fixture(scope="module")
def server_env():
    """
    Startet echten gRPC-Server + HTTP-Status-Server mit gemocktem
    Namensdienst und Worker-Client. Einmal pro Testdatei.
    """
    workers = [WorkerInfo("w1", "localhost", 9001, 0),
               WorkerInfo("w2", "localhost", 9002, 0)]

    store         = TaskStore()
    queue         = TaskQueue()
    ns_client     = _make_ns_client_mock(workers)
    selector      = RoundRobinSelector()
    worker_client = _make_worker_client_mock(accept=True)

    dispatch_loop = DispatchLoop(
        store, queue, ns_client, selector, worker_client,
        timeout_secs=30, max_retries=3,
    )
    dispatch_loop.start()

    collector = StatusCollector(store, ns_client)
    http_srv  = HttpStatusServer(collector, port=HTTP_PORT)
    http_srv.start()

    servicer = DispatcherServicer(store, queue, ns_client, dispatch_loop, collector)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    taskgrid_pb2_grpc.add_DispatcherServiceServicer_to_server(servicer, server)
    server.add_insecure_port(f"localhost:{GRPC_PORT}")
    server.start()

    channel = grpc.insecure_channel(f"localhost:{GRPC_PORT}")
    stub    = taskgrid_pb2_grpc.DispatcherServiceStub(channel)

    time.sleep(0.5)

    yield stub, store, dispatch_loop

    dispatch_loop.stop()
    channel.close()
    server.stop(grace=1)
    time.sleep(0.3)


# ── Issue #13 -- POST_TASK ────────────────────────────────────────────────────

def test_post_task_returns_queued(server_env):
    stub, store, _ = server_env
    resp = _post_task(stub, "sum", "1,2,3", request_id="it-001")
    assert resp.payload.task_id > 0
    assert resp.payload.status == "QUEUED"


def test_post_task_stored_correctly(server_env):
    stub, store, _ = server_env
    resp = _post_task(stub, "sum", "1,2,3", request_id="it-002")
    task = store.get(str(resp.payload.task_id))
    assert task is not None
    assert task.task_type == "sum"
    assert task.payload == "1,2,3"


def test_post_task_empty_type_raises(server_env):
    stub, _, _ = server_env
    with pytest.raises(grpc.RpcError) as exc_info:
        _post_task(stub, "", "x", request_id="it-err")
    assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_post_task_type_too_long_raises(server_env):
    stub, _, _ = server_env
    with pytest.raises(grpc.RpcError) as exc_info:
        _post_task(stub, "x" * 33, "x", request_id="it-err2")
    assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_post_task_payload_too_large_raises(server_env):
    stub, _, _ = server_env
    with pytest.raises(grpc.RpcError) as exc_info:
        _post_task(stub, "sum", "x" * 1025, request_id="it-err3")
    assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_post_task_unique_ids(server_env):
    stub, _, _ = server_env
    ids = {
        _post_task(stub, "hash", str(i), request_id=f"it-uid-{i}").payload.task_id
        for i in range(10)
    }
    assert len(ids) == 10


# ── Issue #14 -- LOOKUP_WORKER / ROUND ROBIN ─────────────────────────────────

def test_round_robin_distributes_evenly():
    workers = [
        WorkerInfo("w1", "localhost", 9001, 0),
        WorkerInfo("w2", "localhost", 9002, 0),
        WorkerInfo("w3", "localhost", 9003, 0),
    ]
    selector   = RoundRobinSelector()
    selections = [selector.select("sum", workers).worker_id for _ in range(9)]
    assert selections == ["w1", "w2", "w3", "w1", "w2", "w3", "w1", "w2", "w3"]


def test_round_robin_empty_returns_none():
    selector = RoundRobinSelector()
    assert selector.select("sum", []) is None


def test_round_robin_independent_counters_per_type():
    workers  = [WorkerInfo("w1", "localhost", 9001, 0),
                WorkerInfo("w2", "localhost", 9002, 0),
                WorkerInfo("w3", "localhost", 9003, 0)]
    selector = RoundRobinSelector()
    sel_a = [selector.select("sum",     workers).worker_id for _ in range(3)]
    sel_b = [selector.select("reverse", workers).worker_id for _ in range(3)]
    assert sel_a == ["w1", "w2", "w3"]
    assert sel_b == ["w1", "w2", "w3"]


# ── Issue #16 -- RETURN_RESULT ────────────────────────────────────────────────

def test_return_result_completed(server_env):
    stub, store, _ = server_env
    resp = _post_task(stub, "sum", "5,5", request_id="rr-001")
    tid  = resp.payload.task_id
    _wait_for_dispatch(store, tid)

    ack = _return_result(stub, tid, "w-sim", "COMPLETED", result="10")
    assert ack.payload.success is True

    task = store.get(str(tid))
    assert task.status == TaskState.COMPLETED
    assert task.result == "10"


def test_return_result_failed(server_env):
    stub, store, _ = server_env
    resp = _post_task(stub, "sum", "err", request_id="rr-002")
    tid  = resp.payload.task_id
    time.sleep(0.5)

    _return_result(stub, tid, "w-sim", "FAILED", error="division by zero")
    task = store.get(str(tid))
    assert task.status == TaskState.FAILED
    assert task.result == "division by zero"


def test_return_result_idempotent(server_env):
    stub, store, _ = server_env
    resp = _post_task(stub, "sum", "1,1", request_id="rr-idem")
    tid  = resp.payload.task_id
    _wait_for_dispatch(store, tid)

    _return_result(stub, tid, "w-sim", "COMPLETED", result="2")
    # Zweites ReturnResult -- Ergebnis darf sich nicht aendern
    ack2 = _return_result(stub, tid, "w-sim", "COMPLETED", result="overwrite")
    assert ack2.payload.success is True
    assert store.get(str(tid)).result == "2"


def test_return_result_unknown_task_id(server_env):
    stub, _, _ = server_env
    ack = _return_result(stub, 99999, "w", "COMPLETED", result="x")
    assert ack.payload.success is False


# ── Issue #17 -- GET_RESULT ───────────────────────────────────────────────────

def test_get_result_completed(server_env):
    stub, store, _ = server_env
    resp = _post_task(stub, "sum", "3,3", request_id="gr-own")
    tid  = resp.payload.task_id
    _wait_for_dispatch(store, tid)

    _return_result(stub, tid, "w-sim", "COMPLETED", result="6")

    gr = _get_result(stub, tid)
    assert gr.payload.status == "COMPLETED"
    assert gr.payload.result == "6"
    assert gr.payload.task_id == tid


def test_get_result_non_terminal_empty_result(server_env):
    stub, _, _ = server_env
    resp   = _post_task(stub, "notype_xyz", "x", request_id="gr-q")
    resp_q = _get_result(stub, resp.payload.task_id)
    assert resp_q.payload.result == ""


def test_get_result_unknown_raises_not_found(server_env):
    stub, _, _ = server_env
    with pytest.raises(grpc.RpcError) as exc_info:
        _get_result(stub, 88888)
    assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND


# ── Issue #18 -- TIMEOUT + RETRY ─────────────────────────────────────────────

def test_timeout_leads_to_failed_after_max_retries():
    """Eigener Server mit kurzem Timeout (1s) und max 2 Retries."""
    store2 = TaskStore()
    queue2 = TaskQueue()
    ns2    = _make_ns_client_mock([WorkerInfo("w1", "localhost", 9999, 0)])
    loop2  = DispatchLoop(store2, queue2, ns2, RoundRobinSelector(),
                          _make_worker_client_mock(accept=True),
                          timeout_secs=1, max_retries=2)
    loop2.start()

    server2 = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    taskgrid_pb2_grpc.add_DispatcherServiceServicer_to_server(
        DispatcherServicer(store2, queue2, ns2, loop2), server2)
    server2.add_insecure_port("localhost:59052")
    server2.start()
    ch2   = grpc.insecure_channel("localhost:59052")
    stub2 = taskgrid_pb2_grpc.DispatcherServiceStub(ch2)

    try:
        resp = _post_task(stub2, "sum", "1", request_id="to-001")
        tid  = str(resp.payload.task_id)
        time.sleep(3.5)  # 2 Timeouts a 1s + Puffer
        task = store2.get(tid)
        assert task is not None
        assert task.status == TaskState.FAILED
        assert task.retry_count >= 2
    finally:
        loop2.stop(); ch2.close(); server2.stop(grace=1); time.sleep(0.3)


def test_timeout_cancelled_when_result_arrives():
    """ReturnResult vor Timeout verhindert Retry."""
    store3 = TaskStore()
    queue3 = TaskQueue()
    ns3    = _make_ns_client_mock([WorkerInfo("w1", "localhost", 9999, 0)])
    loop3  = DispatchLoop(store3, queue3, ns3, RoundRobinSelector(),
                          _make_worker_client_mock(accept=True),
                          timeout_secs=1, max_retries=2)
    loop3.start()

    server3 = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    taskgrid_pb2_grpc.add_DispatcherServiceServicer_to_server(
        DispatcherServicer(store3, queue3, ns3, loop3), server3)
    server3.add_insecure_port("localhost:59053")
    server3.start()
    ch3   = grpc.insecure_channel("localhost:59053")
    stub3 = taskgrid_pb2_grpc.DispatcherServiceStub(ch3)

    try:
        resp = _post_task(stub3, "sum", "2", request_id="to-cancel")
        tid  = str(resp.payload.task_id)
        time.sleep(0.3)
        _return_result(stub3, int(tid), "w1", "COMPLETED", result="ok")
        time.sleep(1.5)
        task = store3.get(tid)
        assert task is not None
        assert task.status == TaskState.COMPLETED
        assert task.retry_count == 0
    finally:
        loop3.stop(); ch3.close(); server3.stop(grace=1); time.sleep(0.3)


# ── Issue #19 -- STRUKTURIERTES LOGGING ──────────────────────────────────────

def test_structured_log_format():
    import logging
    from src.common.logger import get_logger, log_event

    log     = get_logger("integration.test")
    records = []

    class Capture(logging.Handler):
        def emit(self, record):
            records.append(record.getMessage())

    cap = Capture()
    log.addHandler(cap)
    log.setLevel(logging.DEBUG)
    log_event(log, "info", "TEST_EVENT",
              request_id="req-xyz", task_id="42",
              worker_id="w1", status="COMPLETED")
    log.removeHandler(cap)

    assert records, "Kein Log-Record erzeugt"
    msg = records[-1]
    assert "request_id=req-xyz" in msg
    assert "task_id=42"         in msg
    assert "event=TEST_EVENT"   in msg
    assert "worker_id=w1"       in msg
    assert "status=COMPLETED"   in msg


def test_structured_log_writes_to_file():
    import logging
    import tempfile
    import pathlib
    import shutil
    from src.common.logger import get_logger, log_event

    tmpdir = tempfile.mkdtemp()
    try:
        os.environ["LOG_DIR"] = tmpdir
        log2 = get_logger("integration.filetest")
        log_event(log2, "info", "FILE_LOG_TEST", request_id="r1")
        time.sleep(0.2)
        logfiles = list(pathlib.Path(tmpdir).glob("*.log"))
        assert logfiles, f"Keine .log-Datei in {tmpdir}"
        for h in log2.handlers[:]:
            if hasattr(h, "baseFilename"):
                h.close()
                log2.removeHandler(h)
        content = logfiles[0].read_text(encoding="utf-8", errors="replace")
        assert "FILE_LOG_TEST" in content
    finally:
        del os.environ["LOG_DIR"]
        shutil.rmtree(tmpdir, ignore_errors=True)


# ── Issue #20 -- GET_STATUS (gRPC + HTTP) ────────────────────────────────────

def test_get_status_grpc_required_fields(server_env):
    stub, store, _ = server_env
    from src.dispatcher.task import Task

    now = int(time.time())
    for i, (status, dispatched, completed, worker) in enumerate([
        (TaskState.COMPLETED,  now - 2, now - 1, "w1"),
        (TaskState.COMPLETED,  now - 3, now - 1, "w2"),
        (TaskState.FAILED,     now - 1, 0,       "w1"),
        (TaskState.PROCESSING, now,     0,       "w2"),
        (TaskState.QUEUED,     0,       0,       ""),
    ]):
        t = Task(task_id=f"st{i}", task_type="sum" if i % 2 == 0 else "reverse",
                 payload="x", status=status)
        t.timestamp_dispatched = dispatched
        t.timestamp_completed  = completed
        t.assigned_worker      = worker
        t.retry_count          = 1 if i == 2 else 0
        store.add(t)

    time.sleep(0.3)

    resp = stub.GetStatus(taskgrid_pb2.StatusRequest(sender="integration-test"))
    assert resp.payload.queued_tasks >= 1

    data = json.loads(resp.payload.details)
    required = {
        "aktive_worker",
        "unterstützte_tasktypen",
        "offene_tasks",
        "laufende_tasks",
        "abgeschlossene_tasks",
        "fehlgeschlagene_tasks",
        "durchschnittliche_bearbeitungszeit_ms",
        "anzahl_timeouts",
        "anzahl_retries",
    }
    missing = required - data.keys()
    assert not missing, f"Pflichtfelder fehlen: {missing}"
    assert data["abgeschlossene_tasks"] >= 2
    assert data["durchschnittliche_bearbeitungszeit_ms"] >= 0


def test_get_status_http_endpoint(server_env):
    time.sleep(0.5)
    url = f"http://localhost:{HTTP_PORT}/status"
    with urllib.request.urlopen(url, timeout=3) as r:
        body = r.read().decode("utf-8")
        ct   = r.headers.get("Content-Type", "")
    assert "application/json" in ct
    data = json.loads(body)
    assert isinstance(data, dict)
    assert len(data) >= 5


def test_get_status_http_404_for_unknown():
    with pytest.raises(urllib.error.HTTPError) as exc_info:
        urllib.request.urlopen(f"http://localhost:{HTTP_PORT}/unknown", timeout=2)
    assert exc_info.value.code == 404


# ── Issue #21 -- NEBENLAEUIGKEIT ─────────────────────────────────────────────

def test_concurrent_post_task(server_env):
    stub, store, _ = server_env
    results = []
    errors  = []
    lock    = threading.Lock()

    def post(i):
        try:
            r = _post_task(stub, "sum", str(i), request_id=f"cc-{i}", sender="concurrency-test")
            with lock:
                results.append(r.payload.task_id)
        except Exception as e:
            with lock:
                errors.append(str(e))

    threads = [threading.Thread(target=post, args=(i,)) for i in range(50)]
    for t in threads: t.start()
    for t in threads: t.join()

    assert not errors, f"{len(errors)} Fehler: {errors[0]}"
    assert len(set(results)) == 50


def test_concurrent_get_result(server_env):
    stub, store, _ = server_env
    resp      = _post_task(stub, "sum", "0", request_id="cc-sample")
    sample_id = resp.payload.task_id
    errors    = []
    lock      = threading.Lock()

    def get_r():
        try:
            _get_result(stub, sample_id)
        except grpc.RpcError:
            pass
        except Exception as e:
            with lock:
                errors.append(str(e))

    threads = [threading.Thread(target=get_r) for _ in range(50)]
    for t in threads: t.start()
    for t in threads: t.join()

    assert not errors, errors[0]


def test_worker_load_map_consistent(server_env):
    _, store, _ = server_env
    load = store.worker_load_map()
    assert isinstance(load, dict)


# ── DispatchLoop -- Vollstaendiger Durchlauf ──────────────────────────────────

def test_dispatch_loop_full_flow():
    """QUEUED -> DISPATCHED -> PROCESSING -> COMPLETED ueber echten gRPC-Server."""
    store4 = TaskStore()
    queue4 = TaskQueue()
    ns4    = _make_ns_client_mock([WorkerInfo("w1", "localhost", 9001, 0)])
    loop4  = DispatchLoop(store4, queue4, ns4, RoundRobinSelector(),
                          _make_worker_client_mock(accept=True), timeout_secs=10)
    loop4.start()

    server4 = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    taskgrid_pb2_grpc.add_DispatcherServiceServicer_to_server(
        DispatcherServicer(store4, queue4, ns4, loop4), server4)
    server4.add_insecure_port("localhost:59054")
    server4.start()
    ch4   = grpc.insecure_channel("localhost:59054")
    stub4 = taskgrid_pb2_grpc.DispatcherServiceStub(ch4)

    try:
        resp = _post_task(stub4, "reverse", "hello", request_id="flow-1")
        tid  = resp.payload.task_id
        assert resp.payload.status == "QUEUED"

        _wait_for_dispatch(store4, tid)
        task = store4.get(str(tid))
        assert task is not None
        assert task.status in (TaskState.DISPATCHED, TaskState.PROCESSING)
        assert task.assigned_worker == "w1"
        assert task.timestamp_dispatched > 0

        _wait_for_processing(store4, tid)
        _return_result(stub4, tid, "w1", "COMPLETED", result="olleh")
        time.sleep(0.2)

        task = store4.get(str(tid))
        assert task.status == TaskState.COMPLETED
        assert task.result == "olleh"
        assert task.timestamp_completed > 0

        gr = _get_result(stub4, tid)
        assert gr.payload.status == "COMPLETED"
        assert gr.payload.result == "olleh"

    finally:
        loop4.stop(); ch4.close(); server4.stop(grace=1); time.sleep(0.3)
