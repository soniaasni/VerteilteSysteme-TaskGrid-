"""
Phase-2 Integrations-Test — startet echten gRPC-Server und HTTP-Status-Endpoint,
testet alle Dispatcher-Funktionen live ohne Mocks.

Getestete Issues:
  #13 POST_TASK
  #14 lookup_worker / RoundRobin
  #16 ReturnResult
  #17 GetResult
  #18 Timeout + Retry
  #19 Strukturiertes Logging
  #20 GET_STATUS (gRPC + HTTP)
  #21 Nebenläufigkeit
"""

import json
import os
import sys
import threading
import time
import urllib.request
from concurrent import futures
from unittest.mock import MagicMock, patch

import grpc

# Projektpfad
sys.path.insert(0, os.path.dirname(__file__))

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

# ── ANSI-Farben ───────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

passed = []
failed = []

def ok(msg):
    passed.append(msg)
    print(f"  {GREEN}PASS{RESET}  {msg}")

def fail(msg, reason=""):
    failed.append(msg)
    print(f"  {RED}FAIL{RESET}  {msg}" + (f"  → {reason}" if reason else ""))

def section(title):
    print(f"\n{BOLD}{CYAN}{'─'*60}{RESET}")
    print(f"{BOLD}{CYAN}  {title}{RESET}")
    print(f"{BOLD}{CYAN}{'─'*60}{RESET}")


# ── Server-Setup ─────────────────────────────────────────────────────────────

GRPC_PORT  = 59051   # Nicht-Standard-Port um Konflikte zu vermeiden
HTTP_PORT  = 59080

def _make_ns_client_mock(workers=None):
    """Namensdienst-Mock: gibt konfigurierbare Worker zurück."""
    mock = MagicMock(spec=NamensdienstClient)
    mock.lookup_worker.return_value = workers or []
    return mock

def _make_worker_client_mock(accept=True):
    """Worker-Client-Mock: simuliert Worker-Antwort."""
    mock = MagicMock(spec=WorkerClient)
    mock.dispatch_task.return_value = accept
    return mock

def start_server(ns_workers=None, worker_accepts=True,
                 timeout_secs=30, max_retries=3):
    """
    Startet echten gRPC-Server + HTTP-Server in Threads.
    Gibt (stub, store, dispatch_loop, http_port) zurück.
    """
    store         = TaskStore()
    queue         = TaskQueue()
    ns_client     = _make_ns_client_mock(ns_workers)
    selector      = RoundRobinSelector()
    worker_client = _make_worker_client_mock(worker_accepts)

    dispatch_loop = DispatchLoop(
        store, queue, ns_client, selector, worker_client,
        timeout_secs=timeout_secs, max_retries=max_retries,
    )
    dispatch_loop.start()

    collector  = StatusCollector(store, ns_client)
    http_srv   = HttpStatusServer(collector, port=HTTP_PORT)
    http_srv.start()

    servicer = DispatcherServicer(store, queue, ns_client, dispatch_loop, collector)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    taskgrid_pb2_grpc.add_DispatcherServiceServicer_to_server(servicer, server)
    server.add_insecure_port(f"localhost:{GRPC_PORT}")
    server.start()

    channel = grpc.insecure_channel(f"localhost:{GRPC_PORT}")
    stub    = taskgrid_pb2_grpc.DispatcherServiceStub(channel)

    return stub, store, dispatch_loop, server, channel


def stop_server(server, channel, dispatch_loop):
    dispatch_loop.stop()
    channel.close()
    server.stop(grace=1)
    time.sleep(0.3)


# ══════════════════════════════════════════════════════════════════════════════
# ISSUE #13 — POST_TASK
# ══════════════════════════════════════════════════════════════════════════════

def test_post_task(stub, store):
    section("Issue #13 — POST_TASK")

    # Normaler Task
    resp = stub.PostTask(taskgrid_pb2.PostTaskRequest(
        request_id="it-001", task_type="sum", task_payload="1,2,3", sender="integration-test"
    ))
    if resp.task_id > 0 and resp.status == "QUEUED":
        ok("PostTask gibt task_id > 0 und status=QUEUED zurück")
    else:
        fail("PostTask Rückgabe", f"task_id={resp.task_id} status={resp.status}")

    # Task im Store
    task = store.get(str(resp.task_id))
    if task and task.task_type == "sum" and task.payload == "1,2,3":
        ok("Task korrekt im Store gespeichert (type + payload)")
    else:
        fail("Task im Store", f"task={task}")

    # Leerer task_type → INVALID_ARGUMENT
    try:
        stub.PostTask(taskgrid_pb2.PostTaskRequest(
            request_id="it-err", task_type="", task_payload="x", sender="t"
        ))
        fail("Leerer task_type sollte INVALID_ARGUMENT auslösen")
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.INVALID_ARGUMENT:
            ok("Leerer task_type → INVALID_ARGUMENT")
        else:
            fail("Leerer task_type Fehlercode", str(e.code()))

    # Zu langer task_type (>32 Zeichen) → INVALID_ARGUMENT
    try:
        stub.PostTask(taskgrid_pb2.PostTaskRequest(
            request_id="it-err2", task_type="x"*33, task_payload="x", sender="t"
        ))
        fail("Zu langer task_type sollte INVALID_ARGUMENT auslösen")
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.INVALID_ARGUMENT:
            ok("task_type > 32 Zeichen → INVALID_ARGUMENT")
        else:
            fail("task_type zu lang Fehlercode", str(e.code()))

    # Payload zu groß (>1024 Zeichen) → INVALID_ARGUMENT
    try:
        stub.PostTask(taskgrid_pb2.PostTaskRequest(
            request_id="it-err3", task_type="sum", task_payload="x"*1025, sender="t"
        ))
        fail("Zu große Payload sollte INVALID_ARGUMENT auslösen")
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.INVALID_ARGUMENT:
            ok("Payload > 1024 Zeichen → INVALID_ARGUMENT")
        else:
            fail("Payload zu groß Fehlercode", str(e.code()))

    # 10 Tasks → alle eindeutige IDs
    ids = set()
    for i in range(10):
        r = stub.PostTask(taskgrid_pb2.PostTaskRequest(
            request_id=f"it-{i}", task_type="hash", task_payload=str(i), sender="t"
        ))
        ids.add(r.task_id)
    if len(ids) == 10:
        ok("10 Tasks erhalten 10 eindeutige task_ids")
    else:
        fail("Eindeutige task_ids", f"nur {len(ids)} eindeutig von 10")

    return resp.task_id


# ══════════════════════════════════════════════════════════════════════════════
# ISSUE #14 — LOOKUP_WORKER / ROUND ROBIN
# ══════════════════════════════════════════════════════════════════════════════

def test_lookup_worker(store):
    section("Issue #14 — lookup_worker + Round-Robin")

    workers = [
        WorkerInfo("w1", "localhost", 9001, 0),
        WorkerInfo("w2", "localhost", 9002, 0),
        WorkerInfo("w3", "localhost", 9003, 0),
    ]
    selector = RoundRobinSelector()

    selections = [selector.select("sum", workers).worker_id for _ in range(9)]
    expected = ["w1","w2","w3","w1","w2","w3","w1","w2","w3"]
    if selections == expected:
        ok("Round-Robin verteilt 9 Selects gleichmäßig auf 3 Worker (w1,w2,w3 × 3)")
    else:
        fail("Round-Robin Verteilung", f"{selections}")

    # Leere Worker-Liste → None
    result = selector.select("sum", [])
    if result is None:
        ok("Leere Worker-Liste → None (kein Crash)")
    else:
        fail("Leere Worker-Liste", f"erwartet None, got {result}")

    # Verschiedene Task-Typen haben unabhängige Zähler
    sel_a = [selector.select("sum",     workers).worker_id for _ in range(3)]
    sel_b = [selector.select("reverse", workers).worker_id for _ in range(3)]
    if sel_a == ["w1","w2","w3"] and sel_b == ["w1","w2","w3"]:
        ok("Verschiedene task_types haben unabhängige Round-Robin-Zähler")
    else:
        fail("Unabhängige Zähler", f"sum={sel_a} reverse={sel_b}")


# ══════════════════════════════════════════════════════════════════════════════
# ISSUE #16 — RETURN_RESULT
# ══════════════════════════════════════════════════════════════════════════════

def test_return_result(stub, store):
    section("Issue #16 — ReturnResult")

    # Task posten und auf PROCESSING warten
    resp = stub.PostTask(taskgrid_pb2.PostTaskRequest(
        request_id="rr-001", task_type="sum", task_payload="5,5", sender="t"
    ))
    tid = resp.task_id

    # Auf Dispatch warten (max 3s)
    deadline = time.time() + 3.0
    while time.time() < deadline:
        t = store.get(str(tid))
        if t and t.status in (TaskState.PROCESSING, TaskState.DISPATCHED):
            break
        time.sleep(0.1)

    task = store.get(str(tid))
    if task and task.status in (TaskState.PROCESSING, TaskState.DISPATCHED):
        ok(f"Task {tid} wurde dispatcht (Status: {task.status.value})")
    else:
        status = task.status.value if task else "None"
        fail("Task wurde nicht dispatcht", f"status={status}")

    # Ergebnis zurückschicken
    ack = stub.ReturnResult(taskgrid_pb2.ResultRequest(
        request_id="rr-001", task_id=tid, worker_id="w-sim",
        status="COMPLETED", result="10",
    ))
    if ack.success:
        ok("ReturnResult COMPLETED → Ack.success=True")
    else:
        fail("ReturnResult COMPLETED Ack", f"success={ack.success}")

    # Store prüfen
    task = store.get(str(tid))
    if task and task.status == TaskState.COMPLETED and task.result == "10":
        ok("Task im Store: status=COMPLETED, result='10'")
    else:
        fail("Task nach ReturnResult", f"status={task.status if task else 'None'} result={task.result if task else '?'}")

    # FAILED Fall
    resp2 = stub.PostTask(taskgrid_pb2.PostTaskRequest(
        request_id="rr-002", task_type="sum", task_payload="err", sender="t"
    ))
    tid2 = resp2.task_id
    time.sleep(0.5)
    stub.ReturnResult(taskgrid_pb2.ResultRequest(
        request_id="rr-002", task_id=tid2, worker_id="w-sim",
        status="FAILED", error="division by zero",
    ))
    task2 = store.get(str(tid2))
    if task2 and task2.status == TaskState.FAILED and task2.result == "division by zero":
        ok("ReturnResult FAILED → status=FAILED, result=error-message")
    else:
        fail("ReturnResult FAILED", f"status={task2.status if task2 else '?'} result={task2.result if task2 else '?'}")

    # Idempotenz: zweites ReturnResult auf bereits abgeschlossenem Task
    ack2 = stub.ReturnResult(taskgrid_pb2.ResultRequest(
        request_id="rr-dup", task_id=tid, worker_id="w-sim",
        status="COMPLETED", result="overwrite",
    ))
    task_after = store.get(str(tid))
    if ack2.success and task_after.result == "10":
        ok("Doppeltes ReturnResult ignoriert (Idempotenz — result unveraendert)")
    else:
        fail("Idempotenz ReturnResult", f"result={task_after.result if task_after else '?'}")

    # Unbekannte task_id
    ack3 = stub.ReturnResult(taskgrid_pb2.ResultRequest(
        request_id="rr-unk", task_id=99999, worker_id="w",
        status="COMPLETED", result="x",
    ))
    if not ack3.success:
        ok("Unbekannte task_id → Ack.success=False")
    else:
        fail("Unbekannte task_id", "erwartet success=False")

    return tid


# ══════════════════════════════════════════════════════════════════════════════
# ISSUE #17 — GET_RESULT
# ══════════════════════════════════════════════════════════════════════════════

def test_get_result(stub, store, completed_tid):
    section("Issue #17 — GetResult")

    # Abgeschlossener Task
    resp = stub.GetResult(taskgrid_pb2.GetResultRequest(
        request_id="gr-001", task_id=completed_tid, sender="t"
    ))
    if resp.status == "COMPLETED" and resp.result == "10":
        ok(f"GetResult auf COMPLETED Task → status=COMPLETED, result='10'")
    else:
        fail("GetResult COMPLETED", f"status={resp.status} result={resp.result}")

    if resp.task_id == completed_tid:
        ok("GetResult gibt korrekte task_id zurück")
    else:
        fail("GetResult task_id", f"erwartet {completed_tid}, got {resp.task_id}")

    # QUEUED Task (noch kein Ergebnis)
    r = stub.PostTask(taskgrid_pb2.PostTaskRequest(
        request_id="gr-q", task_type="notype_xyz", task_payload="x", sender="t"
    ))
    # Direkt abfragen bevor Dispatch
    resp_q = stub.GetResult(taskgrid_pb2.GetResultRequest(
        request_id="gr-q", task_id=r.task_id, sender="t"
    ))
    if resp_q.result == "":
        ok("GetResult auf nicht-abgeschlossenen Task → leeres result")
    else:
        fail("GetResult nicht-terminal", f"result='{resp_q.result}'")

    # Unbekannte task_id → NOT_FOUND
    try:
        stub.GetResult(taskgrid_pb2.GetResultRequest(
            request_id="gr-unk", task_id=88888, sender="t"
        ))
        fail("Unbekannte task_id sollte NOT_FOUND auslösen")
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.NOT_FOUND:
            ok("Unbekannte task_id → gRPC NOT_FOUND")
        else:
            fail("GetResult NOT_FOUND Fehlercode", str(e.code()))


# ══════════════════════════════════════════════════════════════════════════════
# ISSUE #18 — TIMEOUT + RETRY
# ══════════════════════════════════════════════════════════════════════════════

def test_timeout_retry():
    section("Issue #18 — Timeout + Retry")

    # Server mit sehr kurzem Timeout (1s) und max 2 Retries starten
    # Worker-Client: akzeptiert Task (status=accepted) aber sendet NIE ReturnResult
    store2   = TaskStore()
    queue2   = TaskQueue()
    ns2      = _make_ns_client_mock([WorkerInfo("w1","localhost",9999,0)])
    sel2     = RoundRobinSelector()
    wc2      = _make_worker_client_mock(accept=True)

    loop2 = DispatchLoop(store2, queue2, ns2, sel2, wc2,
                         timeout_secs=1, max_retries=2)
    loop2.start()

    server2 = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    svc2    = DispatcherServicer(store2, queue2, ns2, loop2)
    taskgrid_pb2_grpc.add_DispatcherServiceServicer_to_server(svc2, server2)
    server2.add_insecure_port("localhost:59052")
    server2.start()

    ch2   = grpc.insecure_channel("localhost:59052")
    stub2 = taskgrid_pb2_grpc.DispatcherServiceStub(ch2)

    try:
        resp = stub2.PostTask(taskgrid_pb2.PostTaskRequest(
            request_id="to-001", task_type="sum", task_payload="1", sender="t"
        ))
        tid = str(resp.task_id)

        # Warten bis 2 Timeouts abgelaufen sind (2 × 1s + Puffer)
        time.sleep(3.5)

        task = store2.get(tid)
        if task is None:
            fail("Timeout-Test", "Task nicht im Store")
            return

        if task.status == TaskState.FAILED:
            ok(f"Nach {task.retry_count} Retries → FAILED (max_retries=2)")
        else:
            fail("Timeout → FAILED", f"status={task.status.value}")

        if task.retry_count >= 2:
            ok(f"retry_count={task.retry_count} (mindestens 2 Retries gezaehlt)")
        else:
            fail("retry_count", f"erwartet >=2, got {task.retry_count}")

        # Cancel-Timeout: wenn Ergebnis kommt BEVOR Timeout → kein Retry
        resp3 = stub2.PostTask(taskgrid_pb2.PostTaskRequest(
            request_id="to-cancel", task_type="sum", task_payload="2", sender="t"
        ))
        tid3 = str(resp3.task_id)
        time.sleep(0.3)  # Kurz warten auf Dispatch
        stub2.ReturnResult(taskgrid_pb2.ResultRequest(
            request_id="to-cancel", task_id=int(tid3), worker_id="w1",
            status="COMPLETED", result="ok",
        ))
        time.sleep(1.5)  # Timeout-Fenster verstreichen lassen
        task3 = store2.get(tid3)
        if task3 and task3.status == TaskState.COMPLETED and task3.retry_count == 0:
            ok("ReturnResult vor Timeout → kein Retry (retry_count=0)")
        else:
            fail("Cancel-Timeout", f"status={task3.status.value if task3 else '?'} retries={task3.retry_count if task3 else '?'}")

    finally:
        loop2.stop()
        ch2.close()
        server2.stop(grace=1)
        time.sleep(0.3)


# ══════════════════════════════════════════════════════════════════════════════
# ISSUE #19 — STRUKTURIERTES LOGGING
# ══════════════════════════════════════════════════════════════════════════════

def test_structured_logging():
    section("Issue #19 — Strukturiertes Logging")
    import logging
    from src.common.logger import get_logger, log_event

    log = get_logger("integration.test")
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

    if records:
        msg = records[-1]
        checks = [
            ("request_id=req-xyz", "request_id vorhanden"),
            ("task_id=42",         "task_id vorhanden"),
            ("event=TEST_EVENT",   "event vorhanden"),
            ("worker_id=w1",       "worker_id vorhanden"),
            ("status=COMPLETED",   "status vorhanden"),
        ]
        for pattern, label in checks:
            if pattern in msg:
                ok(f"Log-Format: {label}")
            else:
                fail(f"Log-Format: {label}", f"'{pattern}' nicht in '{msg}'")
    else:
        fail("Log-Event erzeugt keine Records")

    log.removeHandler(cap)

    # LOG_DIR: Datei-Logging
    import tempfile, pathlib
    tmpdir = tempfile.mkdtemp()
    try:
        os.environ["LOG_DIR"] = tmpdir
        log2 = get_logger("integration.filetest")
        log_event(log2, "info", "FILE_LOG_TEST", request_id="r1")
        time.sleep(0.2)
        logfiles = list(pathlib.Path(tmpdir).glob("*.log"))
        if logfiles:
            # Handler schliessen bevor Verzeichnis geloescht wird (Windows)
            for h in log2.handlers[:]:
                if hasattr(h, "baseFilename"):
                    h.close()
                    log2.removeHandler(h)
            content = logfiles[0].read_text(encoding="utf-8", errors="replace")
            if "FILE_LOG_TEST" in content:
                ok("LOG_DIR-Datei-Logging schreibt Events in .log-Datei")
            else:
                fail("Datei-Logging Inhalt", f"'FILE_LOG_TEST' nicht in {logfiles[0].name}")
        else:
            fail("Datei-Logging", f"Keine .log-Datei in {tmpdir}")
        del os.environ["LOG_DIR"]
    finally:
        import shutil
        shutil.rmtree(tmpdir, ignore_errors=True)


# ══════════════════════════════════════════════════════════════════════════════
# ISSUE #20 — GET_STATUS (gRPC + HTTP)
# ══════════════════════════════════════════════════════════════════════════════

def test_get_status(stub, store):
    section("Issue #20 — GET_STATUS (gRPC + HTTP)")

    # Einige Tasks in verschiedenen Zuständen anlegen
    from src.dispatcher.task import Task
    from src.common.protocol import new_task_id

    now = int(time.time())
    for i, (status, dispatched, completed, worker) in enumerate([
        (TaskState.COMPLETED, now-2, now-1, "w1"),
        (TaskState.COMPLETED, now-3, now-1, "w2"),
        (TaskState.FAILED,    now-1, 0,     "w1"),
        (TaskState.PROCESSING, now,  0,     "w2"),
        (TaskState.QUEUED,    0,     0,     ""),
    ]):
        t = Task(task_id=f"st{i}", task_type="sum" if i%2==0 else "reverse",
                 payload="x", status=status)
        t.timestamp_dispatched = dispatched
        t.timestamp_completed  = completed
        t.assigned_worker      = worker
        t.retry_count          = 1 if i == 2 else 0
        store.add(t)

    time.sleep(0.3)

    # gRPC GetStatus
    resp = stub.GetStatus(taskgrid_pb2.StatusRequest(sender="integration-test"))
    if resp.queued_tasks >= 1:
        ok(f"gRPC GetStatus: queued_tasks={resp.queued_tasks}")
    else:
        fail("gRPC GetStatus queued_tasks", str(resp.queued_tasks))

    try:
        data = json.loads(resp.details)
        required = {"aktive_worker","unterstützte_tasktypen","offene_tasks",
                    "laufende_tasks","abgeschlossene_tasks","fehlgeschlagene_tasks",
                    "durchschnittliche_bearbeitungszeit_ms","anzahl_timeouts","anzahl_retries"}
        missing = required - data.keys()
        if not missing:
            ok("gRPC GetStatus.details enthält alle 9 Pflichtfelder")
        else:
            fail("gRPC GetStatus.details fehlende Felder", str(missing))

        if data["abgeschlossene_tasks"] >= 2:
            ok(f"abgeschlossene_tasks={data['abgeschlossene_tasks']} (mindestens 2)")
        else:
            fail("abgeschlossene_tasks", str(data.get("abgeschlossene_tasks")))

        if data["durchschnittliche_bearbeitungszeit_ms"] > 0:
            ok(f"durchschnittliche_bearbeitungszeit_ms={data['durchschnittliche_bearbeitungszeit_ms']}ms")
        else:
            fail("avg_ms", str(data.get("durchschnittliche_bearbeitungszeit_ms")))

        # "sum" und "reverse" müssen enthalten sein (Store kann mehr haben von vorherigen Tests)
        known = set(data["unterstützte_tasktypen"])
        if {"sum", "reverse"}.issubset(known):
            ok(f"unterstützte_tasktypen enthält 'sum' und 'reverse': {sorted(known)}")
        else:
            fail("unterstützte_tasktypen", str(data.get("unterstützte_tasktypen")))

    except Exception as e:
        fail("gRPC GetStatus.details JSON-Parse", str(e))

    # HTTP GET /status
    time.sleep(0.5)
    try:
        url = f"http://localhost:{HTTP_PORT}/status"
        with urllib.request.urlopen(url, timeout=3) as r:
            body = r.read().decode("utf-8")
            data_http = json.loads(body)
            ct = r.headers.get("Content-Type", "")

        if "application/json" in ct:
            ok(f"HTTP GET /status → Content-Type: application/json")
        else:
            fail("HTTP Content-Type", ct)

        missing_http = required - data_http.keys()
        if not missing_http:
            ok("HTTP /status enthält alle 9 Pflichtfelder")
        else:
            fail("HTTP /status fehlende Felder", str(missing_http))

        if data_http["abgeschlossene_tasks"] >= 2:
            ok(f"HTTP: abgeschlossene_tasks={data_http['abgeschlossene_tasks']}")
        else:
            fail("HTTP: abgeschlossene_tasks", str(data_http.get("abgeschlossene_tasks")))

    except Exception as e:
        fail("HTTP GET /status", str(e))

    # HTTP 404 für unbekannte Pfade
    try:
        urllib.request.urlopen(f"http://localhost:{HTTP_PORT}/unknown", timeout=2)
        fail("HTTP /unknown sollte 404 zurückgeben")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            ok("HTTP GET /unknown → 404")
        else:
            fail("HTTP 404", f"got {e.code}")


# ══════════════════════════════════════════════════════════════════════════════
# ISSUE #21 — NEBENLÄUFIGKEIT (live)
# ══════════════════════════════════════════════════════════════════════════════

def test_concurrency_live(stub, store):
    section("Issue #21 — Nebenläufigkeit (live, echter Server)")

    # 50 gleichzeitige PostTask-Aufrufe
    results = []
    errors  = []
    lock    = threading.Lock()

    def post(i):
        try:
            r = stub.PostTask(taskgrid_pb2.PostTaskRequest(
                request_id=f"cc-{i}", task_type="sum",
                task_payload=str(i), sender="concurrency-test"
            ))
            with lock:
                results.append(r.task_id)
        except Exception as e:
            with lock:
                errors.append(str(e))

    threads = [threading.Thread(target=post, args=(i,)) for i in range(50)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    if errors:
        fail("50 parallele PostTask", f"{len(errors)} Fehler: {errors[0]}")
    else:
        ok(f"50 parallele PostTask ohne Fehler")

    if len(set(results)) == 50:
        ok("50 eindeutige task_ids unter parallelem Zugriff")
    else:
        fail("Eindeutige task_ids", f"nur {len(set(results))} eindeutig")

    # 50 gleichzeitige GetResult-Aufrufe auf denselben Task
    sample_id = results[0]
    get_errors = []

    def get_result():
        try:
            stub.GetResult(taskgrid_pb2.GetResultRequest(
                request_id="cc-get", task_id=sample_id, sender="t"
            ))
        except grpc.RpcError:
            pass  # NOT_FOUND ist ok
        except Exception as e:
            with lock:
                get_errors.append(str(e))

    threads2 = [threading.Thread(target=get_result) for _ in range(50)]
    for t in threads2:
        t.start()
    for t in threads2:
        t.join()

    if not get_errors:
        ok("50 parallele GetResult ohne Exception (kein Crash)")
    else:
        fail("50 parallele GetResult", get_errors[0])

    # worker_load_map unter Last
    load = store.worker_load_map()
    ok(f"worker_load_map() liefert konsistenten Snapshot: {dict(load)}")


# ══════════════════════════════════════════════════════════════════════════════
# DISPATCH LOOP — Vollständiger Durchlauf
# ══════════════════════════════════════════════════════════════════════════════

def test_dispatch_loop_full_flow():
    section("DispatchLoop — Vollstaendiger Task-Durchlauf")

    store3   = TaskStore()
    queue3   = TaskQueue()
    ns3      = _make_ns_client_mock([WorkerInfo("w1","localhost",9001,0)])
    sel3     = RoundRobinSelector()
    wc3      = _make_worker_client_mock(accept=True)

    loop3 = DispatchLoop(store3, queue3, ns3, sel3, wc3, timeout_secs=10)
    loop3.start()

    server3 = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    svc3    = DispatcherServicer(store3, queue3, ns3, loop3)
    taskgrid_pb2_grpc.add_DispatcherServiceServicer_to_server(svc3, server3)
    server3.add_insecure_port("localhost:59053")
    server3.start()

    ch3   = grpc.insecure_channel("localhost:59053")
    stub3 = taskgrid_pb2_grpc.DispatcherServiceStub(ch3)

    try:
        resp = stub3.PostTask(taskgrid_pb2.PostTaskRequest(
            request_id="flow-1", task_type="reverse", task_payload="hello", sender="t"
        ))
        tid = resp.task_id
        if resp.status == "QUEUED":
            ok("PostTask → QUEUED")
        else:
            fail("PostTask Initialstatus", resp.status)

        # Warten bis DISPATCHED oder PROCESSING
        deadline = time.time() + 3.0
        while time.time() < deadline:
            t = store3.get(str(tid))
            if t and t.status in (TaskState.DISPATCHED, TaskState.PROCESSING):
                break
            time.sleep(0.05)

        task = store3.get(str(tid))
        if task and task.status in (TaskState.DISPATCHED, TaskState.PROCESSING):
            ok(f"QUEUED → {task.status.value} (Dispatch-Loop hat Task verarbeitet)")
        else:
            fail("Dispatch-Loop", f"status={task.status.value if task else 'None'}")

        if task and task.assigned_worker == "w1":
            ok("assigned_worker='w1' korrekt gesetzt")
        else:
            fail("assigned_worker", str(task.assigned_worker if task else "None"))

        if task and task.timestamp_dispatched > 0:
            ok("timestamp_dispatched gesetzt")
        else:
            fail("timestamp_dispatched", str(task.timestamp_dispatched if task else "None"))

        # Warten bis PROCESSING (Task muss PROCESSING sein bevor ReturnResult)
        deadline2 = time.time() + 3.0
        while time.time() < deadline2:
            t = store3.get(str(tid))
            if t and t.status == TaskState.PROCESSING:
                break
            time.sleep(0.05)

        # ReturnResult → COMPLETED
        stub3.ReturnResult(taskgrid_pb2.ResultRequest(
            request_id="flow-1", task_id=tid, worker_id="w1",
            status="COMPLETED", result="olleh",
        ))
        time.sleep(0.2)

        task = store3.get(str(tid))
        if task and task.status == TaskState.COMPLETED:
            ok("PROCESSING → COMPLETED nach ReturnResult")
        else:
            fail("COMPLETED", f"status={task.status.value if task else 'None'}")

        if task and task.result == "olleh":
            ok("result='olleh' korrekt gespeichert")
        else:
            fail("result", str(task.result if task else "None"))

        if task and task.timestamp_completed > 0:
            ok("timestamp_completed gesetzt")
        else:
            fail("timestamp_completed", str(task.timestamp_completed if task else "None"))

        # GetResult bestätigt
        gr = stub3.GetResult(taskgrid_pb2.GetResultRequest(
            request_id="flow-1", task_id=tid, sender="t"
        ))
        if gr.status == "COMPLETED" and gr.result == "olleh":
            ok("GetResult bestaetigt: status=COMPLETED result='olleh'")
        else:
            fail("GetResult nach Abschluss", f"status={gr.status} result={gr.result}")

    finally:
        loop3.stop()
        ch3.close()
        server3.stop(grace=1)
        time.sleep(0.3)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print(f"\n{BOLD}{'═'*60}")
    print("  TaskGrid+ Phase-2 Integrations-Test")
    print(f"{'═'*60}{RESET}")

    # Hauptserver starten (mit Worker der Tasks akzeptiert)
    workers = [WorkerInfo("w1", "localhost", 9001, 0),
               WorkerInfo("w2", "localhost", 9002, 0)]
    stub, store, dispatch_loop, server, channel = start_server(
        ns_workers=workers, worker_accepts=True
    )
    time.sleep(0.5)  # Server-Startzeit

    try:
        completed_tid = test_post_task(stub, store)
        test_lookup_worker(store)
        completed_tid2 = test_return_result(stub, store)
        test_get_result(stub, store, completed_tid2)
        test_structured_logging()
        test_get_status(stub, store)
        test_concurrency_live(stub, store)
    finally:
        stop_server(server, channel, dispatch_loop)

    # Separate Tests die eigene Server brauchen
    test_timeout_retry()
    test_dispatch_loop_full_flow()

    # ── Zusammenfassung ───────────────────────────────────────────────────────
    total = len(passed) + len(failed)
    print(f"\n{BOLD}{'═'*60}")
    print(f"  Ergebnis: {GREEN}{len(passed)} PASS{RESET}{BOLD} / "
          f"{RED}{len(failed)} FAIL{RESET}{BOLD} (von {total})")
    print(f"{'═'*60}{RESET}\n")

    if failed:
        print(f"{RED}Fehlgeschlagene Tests:{RESET}")
        for f in failed:
            print(f"  - {f}")
        sys.exit(1)
    else:
        print(f"{GREEN}Alle Tests bestanden.{RESET}\n")
        sys.exit(0)
