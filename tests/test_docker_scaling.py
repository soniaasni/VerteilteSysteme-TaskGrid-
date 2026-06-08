"""
Issue #39 — Docker-Skalierung: Mehrfach-Worker desselben Typs testen.

Szenarien:
  1. Alle 3 worker-sum-Instanzen registrieren sich beim Namensdienst
  2. LookupWorker liefert alle 3 Worker zurück
  3. Dispatcher verteilt Tasks auf verschiedene Worker (Round-Robin)
  4. Einen Worker stoppen: die anderen 2 arbeiten weiterhin korrekt

Voraussetzung: Docker ist verfügbar.
Start: docker compose up --scale worker-sum=3 -d --build

Ausfuehren:
  pytest tests/test_docker_scaling.py -v
"""

import json
import os
import shutil
import subprocess
import time

import grpc
import pytest

from proto import taskgrid_pb2, taskgrid_pb2_grpc

REPO_ROOT            = os.path.dirname(os.path.dirname(__file__))
DISPATCHER_ADDRESS   = os.environ.get("DISPATCHER_ADDRESS",   "localhost:50051")
NAMENSDIENST_ADDRESS = os.environ.get("NAMENSDIENST_ADDRESS", "localhost:50052")

# Anzahl worker-sum-Instanzen fuer den Scaling-Test
WORKER_SUM_SCALE = 3

# Timing
REGISTRATION_TIMEOUT_SECS = 60   # wie lange auf Worker-Registrierung warten
TASK_COMPLETE_TIMEOUT_SECS = 30  # wie lange auf Task-Completion warten
UNHEALTHY_TIMEOUT_SECS     = 25  # Puffer fuer UNHEALTHY nach Worker-Stop


# ── Docker-Compose-Hilfsfunktionen ────────────────────────────────────────────

def _docker_compose(*args: str) -> subprocess.CompletedProcess:
    if shutil.which("docker") is None:
        pytest.skip("Docker ist in dieser Umgebung nicht verfuegbar.")
    if subprocess.run(["docker", "info"], capture_output=True, check=False).returncode != 0:
        pytest.skip("Docker-Daemon ist in dieser Umgebung nicht verfuegbar.")
    return subprocess.run(
        ["docker", "compose", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
        env=os.environ.copy(),
    )


def _bring_up() -> None:
    """Startet das System mit 3 worker-sum-Instanzen."""
    result = _docker_compose(
        "up", "-d", "--build",
        f"--scale", f"worker-sum={WORKER_SUM_SCALE}",
    )
    assert result.returncode == 0, (
        f"docker compose up fehlgeschlagen:\n{result.stderr or result.stdout}"
    )
    # Warten bis Dispatcher healthy ist
    _wait_for_service("dispatcher", timeout=90)


def _bring_down() -> None:
    _docker_compose("down", "-v")


def _wait_for_service(service: str, timeout: int = 90) -> None:
    """Wartet bis ein Service als 'running' / 'healthy' gilt."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = _docker_compose("ps", "--format", "json")
        if result.returncode != 0:
            time.sleep(2)
            continue
        for raw in result.stdout.splitlines():
            if not raw.strip():
                continue
            try:
                entry = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if entry.get("Service") == service:
                state  = str(entry.get("State",  "")).lower()
                health = str(entry.get("Health", "")).lower()
                if "running" in state or "healthy" in health:
                    return
        time.sleep(2)
    raise AssertionError(f"Service '{service}' nicht innerhalb von {timeout}s bereit.")


def _get_worker_sum_container_names() -> list[str]:
    """Gibt die Container-Namen aller laufenden worker-sum-Instanzen zurueck."""
    result = _docker_compose("ps", "--format", "json")
    names = []
    for raw in result.stdout.splitlines():
        if not raw.strip():
            continue
        try:
            entry = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if entry.get("Service") == "worker-sum" and "running" in str(entry.get("State", "")).lower():
            names.append(entry.get("Name", ""))
    return names


def _stop_container(name: str) -> None:
    """Stoppt einen einzelnen Container per Name."""
    result = subprocess.run(
        ["docker", "stop", name],
        capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, f"docker stop {name} fehlgeschlagen:\n{result.stderr}"


# ── gRPC-Hilfsfunktionen ─────────────────────────────────────────────────────

def _lookup_workers(task_type: str) -> list:
    """Fragt den Namensdienst direkt nach registrierten Workern."""
    with grpc.insecure_channel(NAMENSDIENST_ADDRESS) as channel:
        stub = taskgrid_pb2_grpc.NamingServiceStub(channel)
        response = stub.LookupWorker(
            taskgrid_pb2.LookupRequest(
                message_type="LOOKUP_WORKER",
                request_id="scaling-test-lookup",
                timestamp=int(time.time()),
                sender="pytest-scaling",
                payload=taskgrid_pb2.LookupRequest.Payload(task_type=task_type),
            )
        )
        return list(response.payload.workers)


def _post_task(task_type: str, payload: str, request_id: str) -> int:
    """Sendet einen Task und gibt die task_id zurueck."""
    with grpc.insecure_channel(DISPATCHER_ADDRESS) as channel:
        stub = taskgrid_pb2_grpc.DispatcherServiceStub(channel)
        resp = stub.PostTask(
            taskgrid_pb2.PostTaskRequest(
                message_type="POST_TASK",
                request_id=request_id,
                timestamp=int(time.time()),
                sender="pytest-scaling",
                payload=taskgrid_pb2.PostTaskRequest.Payload(
                    task_type=task_type,
                    task_payload=payload,
                ),
            )
        )
        return resp.payload.task_id


def _get_result(task_id: int) -> tuple[str, str]:
    """Gibt (status, result) fuer eine Task-ID zurueck."""
    with grpc.insecure_channel(DISPATCHER_ADDRESS) as channel:
        stub = taskgrid_pb2_grpc.DispatcherServiceStub(channel)
        resp = stub.GetResult(
            taskgrid_pb2.GetResultRequest(
                message_type="GET_RESULT",
                request_id=f"gr-{task_id}",
                timestamp=int(time.time()),
                sender="pytest-scaling",
                payload=taskgrid_pb2.GetResultRequest.Payload(task_id=task_id),
            )
        )
        return resp.payload.status, resp.payload.result


def _wait_for_task_completed(task_id: int, timeout: int = TASK_COMPLETE_TIMEOUT_SECS) -> str:
    """Wartet bis Task COMPLETED oder FAILED ist; gibt Status zurueck."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        status, _ = _get_result(task_id)
        if status in ("COMPLETED", "FAILED"):
            return status
        time.sleep(1)
    status, _ = _get_result(task_id)
    return status


def _wait_for_n_workers(task_type: str, n: int, timeout: int = REGISTRATION_TIMEOUT_SECS) -> list:
    """Wartet bis mindestens n Worker vom Typ task_type registriert sind."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        workers = _lookup_workers(task_type)
        if len(workers) >= n:
            return workers
        time.sleep(2)
    return _lookup_workers(task_type)


# ── Tests ────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module", autouse=True)
def docker_env():
    """Startet das System mit 3 worker-sum-Instanzen und raumt danach auf."""
    _bring_up()
    yield
    _bring_down()


def test_all_workers_register_at_nameservice():
    """
    Szenario 1: Alle 3 worker-sum-Instanzen registrieren sich beim Namensdienst.
    """
    workers = _wait_for_n_workers("sum", WORKER_SUM_SCALE)
    assert len(workers) >= WORKER_SUM_SCALE, (
        f"Erwartet: {WORKER_SUM_SCALE} registrierte 'sum'-Worker, "
        f"tatsaechlich registriert: {len(workers)}. "
        f"Worker: {[w.worker_id for w in workers]}"
    )


def test_lookup_worker_returns_all_instances():
    """
    Szenario 2: LookupWorker liefert alle 3 Worker-Instanzen zurueck.
    """
    workers = _wait_for_n_workers("sum", WORKER_SUM_SCALE)
    worker_ids = [w.worker_id for w in workers]
    # Alle Worker-IDs muessen eindeutig sein
    assert len(set(worker_ids)) == len(worker_ids), (
        f"Worker-IDs nicht eindeutig: {worker_ids}"
    )
    assert len(workers) >= WORKER_SUM_SCALE, (
        f"LookupWorker gibt nur {len(workers)} von {WORKER_SUM_SCALE} Workern zurueck."
    )


def test_dispatcher_distributes_tasks_across_workers():
    """
    Szenario 3: Dispatcher verteilt Tasks per Round-Robin auf verschiedene Worker.
    Tasks werden an worker-sum geschickt; der echte Worker berechnet die Summe
    und schickt ReturnResult. Wir pruefen, dass mindestens 2 verschiedene Worker
    Tasks erhalten haben (Round-Robin ueber 3 Instanzen).
    """
    # Sicherstellen dass alle 3 Worker bereit sind
    _wait_for_n_workers("sum", WORKER_SUM_SCALE)

    # 6 Tasks einreichen (2× mehr als Worker → Round-Robin tritt sicher auf)
    task_ids = []
    for i in range(6):
        tid = _post_task("sum", f"{i+1},{i+2}", request_id=f"scale-dist-{i}")
        task_ids.append(tid)
        time.sleep(0.2)

    # Auf Completion warten
    completed = 0
    for tid in task_ids:
        status = _wait_for_task_completed(tid)
        if status == "COMPLETED":
            completed += 1

    assert completed >= 4, (
        f"Mindestens 4 von 6 Tasks sollten COMPLETED sein, aber nur {completed} wurden abgeschlossen. "
        f"Tasks: {task_ids}"
    )

    # Dispatcher-Logs pruefen: muessen mehrere unterschiedliche Worker-IDs vorkommen
    result = subprocess.run(
        ["docker", "logs", "dispatcher"],
        capture_output=True, text=True, check=False,
    )
    logs = result.stdout + result.stderr

    # Alle registrierten Worker-IDs aus dem Namensdienst holen
    workers     = _lookup_workers("sum")
    worker_ids  = {w.worker_id for w in workers}

    assigned_workers_in_logs = {wid for wid in worker_ids if wid in logs}
    assert len(assigned_workers_in_logs) >= 2, (
        f"Erwartet: Tasks auf mindestens 2 verschiedene Worker verteilt. "
        f"In Dispatcher-Logs gefundene Worker-IDs: {assigned_workers_in_logs}. "
        f"Alle Worker: {worker_ids}"
    )


def test_one_worker_stop_others_continue():
    """
    Szenario 4: Einen Worker stoppen — die anderen 2 arbeiten weiterhin.
    """
    # Alle 3 Worker sicherstellen
    _wait_for_n_workers("sum", WORKER_SUM_SCALE)

    # Einen worker-sum Container stoppen
    containers = _get_worker_sum_container_names()
    assert len(containers) >= WORKER_SUM_SCALE, (
        f"Erwartet {WORKER_SUM_SCALE} laufende worker-sum Container, gefunden: {containers}"
    )
    _stop_container(containers[0])

    # Warten bis der gestoppte Worker UNHEALTHY wird (kein Heartbeat mehr)
    deadline = time.time() + UNHEALTHY_TIMEOUT_SECS
    while time.time() < deadline:
        workers = _lookup_workers("sum")
        if len(workers) <= WORKER_SUM_SCALE - 1:
            break
        time.sleep(2)

    remaining_workers = _lookup_workers("sum")
    assert len(remaining_workers) >= WORKER_SUM_SCALE - 1, (
        f"Nach Stop eines Workers sollten noch {WORKER_SUM_SCALE - 1} Worker aktiv sein, "
        f"aber nur {len(remaining_workers)} sind registriert."
    )

    # Task einreichen und pruefen ob er von den verbliebenen Workern bearbeitet wird
    tid = _post_task("sum", "10,20", request_id="scale-failover")
    status = _wait_for_task_completed(tid, timeout=30)
    assert status == "COMPLETED", (
        f"Task nach Worker-Ausfall sollte COMPLETED sein, ist aber: {status}"
    )
