"""
Fehlertest: Abrupter Worker-Absturz — Worker stirbt mitten in der Verarbeitung
ohne DEREGISTER (kein graceful shutdown).

Architektonischer Hintergrund
─────────────────────────────
Der Worker ist SYNCHRON: ExecuteTask() schläft (wait-Handler), ruft dann
ReturnResult() auf und schickt erst danach den gRPC-ACK zurück.

Mit wait=30s überschreitet die Antwortzeit den gRPC-Timeout des WorkerClient
(DISPATCH_TIMEOUT_SECS = 5.0s hardcoded). Das bedeutet:

  t=0s   Task → DISPATCHED, Dispatcher-Timer(3s) startet, gRPC-Call beginnt.
  t=1s   docker kill worker-wait → TCP-Verbindung bricht sofort ab.
  t=3s   Dispatcher-Timer feuert:
           Status ist noch DISPATCHED (gRPC-Fehler noch nicht eingetroffen,
           weil der Client erst nach dem eigenen 5s-Timeout aufgibt) ODER
           der gRPC-Fehler ist bereits eingetroffen und hat FAILED gesetzt.
           Zwei mögliche Pfade — beide sind korrektes Verhalten:

         Pfad A — Timer gewinnt (t_kill-Latenz + gRPC-Stack-Overhead > 3s):
           DISPATCHED → TIMEOUT → RETRYING → re-enqueue → neuer Dispatch
           versucht den toten Worker → gRPC UNAVAILABLE → stale-guard → ignoriert
           Timer des 2. Zyklus feuert → RETRYING → FAILED (max_retries erreicht)
           Log: DISPATCH_TIMEOUT_fired, TIMEOUT_retrying, TIMEOUT_max_retries_exceeded

         Pfad B — gRPC-Fehler gewinnt (Verbindungsabbruch kommt vor Timer):
           gRPC UNAVAILABLE → stale-guard: Task noch DISPATCHED, gleicher
           timestamp_dispatched → _cancel_timeout() → FAILED direkt
           Log: DISPATCH_worker_unreachable, kein TIMEOUT-Event

  t=5s   Falls gRPC-Timeout zieht: DEADLINE_EXCEEDED → stale-guard prüft
         aktuellen Zustand; ignoriert falls Timer schon gewonnen hat.

Entscheidend ist: Der Task endet in jedem Fall FAILED (kein COMPLETED) und
das System bleibt danach stabil.

Namensdienst-Sicht:
  - Heartbeat-Intervall des Workers: 5s
  - Namensdienst markiert Worker UNHEALTHY nach 5s ohne Heartbeat
  - Namensdienst entfernt Worker (OFFLINE) nach 10s ohne Heartbeat (hardcoded: 5s * 2)
  - Nach dem docker kill: keine weiteren Heartbeats → innerhalb von ~15s OFFLINE

Parameter:
  DISPATCH_TIMEOUT_SECONDS = 3   (Dispatcher-Timer, < gRPC-Timeout 5s)
  MAX_RETRIES              = 2
  wait                     = 30  (Worker schläft 30s, stirbt nach 1s durch kill)
  HEARTBEAT_INTERVAL       = 5s  (Worker-Default)
  NAMESERVICE_OFFLINE_SECS = 10  (5s × 2, hardcoded in nameservice.py::startLoop)

Erwartetes Verhalten (Pfad A — reproduzierbarer Hauptpfad):
  1. Task wird dispatcht: QUEUED → DISPATCHED
  2. Worker stirbt abrupt — kein DEREGISTER, kein ReturnResult
  3. Task endet in FAILED (via Timer-Kette oder direkter gRPC-Fehler)
  4. Namensdienst markiert Worker nach ausgebliebenen Heartbeats als OFFLINE
  5. Dispatcher bleibt stabil — neuer Task (anderer Typ) wird COMPLETED
"""

import json
import os
import shutil
import subprocess
import tempfile
import time
from typing import Optional

import grpc
import pytest

from proto import taskgrid_pb2, taskgrid_pb2_grpc


REPO_ROOT             = os.path.dirname(os.path.dirname(__file__))
DISPATCHER_ADDRESS    = os.environ.get("DISPATCHER_ADDRESS", "localhost:50051")
NAMENSDIENST_ADDRESS  = os.environ.get("NAMENSDIENST_ADDRESS", "localhost:50052")

# ── Timing-Konstanten ─────────────────────────────────────────────────────────
#
# Hardcodiert im System (nicht änderbar ohne Code-Änderung):
#   GRPC_WORKER_CLIENT_TIMEOUT  = 5.0s  (worker_client.py)
#   NAMESERVICE_UNHEALTHY_SECS  = 5s    (nameservice.py::startLoop(5, 2))
#   NAMESERVICE_OFFLINE_SECS    = 10s   (nameservice.py::startLoop(5, 2) → 5*2)
#
# Via docker-compose-Override konfiguriert:
#   DISPATCH_TIMEOUT_SECONDS  = 3s  (Dispatcher-Timer, muss < 5s)
#   MAX_RETRIES               = 2
#
GRPC_WORKER_CLIENT_TIMEOUT  = 5.0
NAMESERVICE_UNHEALTHY_SECS  = 5
NAMESERVICE_OFFLINE_SECS    = 10   # 5s * 2 (hardcoded Multiplikator)

TEST_DISPATCH_TIMEOUT_SECS  = 3    # Dispatcher-Timer; muss < GRPC_WORKER_CLIENT_TIMEOUT
TEST_MAX_RETRIES             = 2
WAIT_TASK_DURATION_SECS      = 30  # weit über GRPC_WORKER_CLIENT_TIMEOUT → ACK kommt nie

WORKER_KILL_DELAY_SECS       = 1   # kurz nach Dispatch killen (Task sicher im gRPC-Call)

# Budget für einen Retry-Zyklus: Timer + Overhead
_SECS_PER_CYCLE              = TEST_DISPATCH_TIMEOUT_SECS + 4
# Gesamtbudget bis FAILED: alle Zyklen + gRPC-Timeout-Overhead + Puffer
TERMINAL_POLL_TIMEOUT_SECS   = _SECS_PER_CYCLE * (TEST_MAX_RETRIES + 1) + 20
# Budget für Stabilitäts-Task (schneller Worker-Typ)
STABILITY_POLL_TIMEOUT_SECS  = 25
# Budget für Namensdienst-OFFLINE-Check: OFFLINE nach ~10s + Puffer
NAMESERVICE_OFFLINE_POLL_SECS = NAMESERVICE_OFFLINE_SECS + 10


# ── Override-Datei ─────────────────────────────────────────────────────────────

OVERRIDE_CONTENT = f"""\
# Temporary override for error_test_worker_crash.py
services:
  dispatcher:
    environment:
      - DISPATCH_TIMEOUT_SECONDS={TEST_DISPATCH_TIMEOUT_SECS}
      - MAX_RETRIES={TEST_MAX_RETRIES}
"""


# ── Docker-Compose-Hilfsfunktionen ────────────────────────────────────────────

def _docker_compose(*args: str, extra_env: Optional[dict] = None) -> subprocess.CompletedProcess:
    """Führt `docker compose …` aus und gibt das Ergebnis zurück."""
    if shutil.which("docker") is None:
        pytest.skip("Docker ist in dieser Umgebung nicht verfügbar.")

    if subprocess.run(["docker", "info"], capture_output=True, check=False).returncode != 0:
        pytest.skip("Docker-Daemon ist in dieser Umgebung nicht verfügbar.")

    return subprocess.run(
        ["docker", "compose", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, **(extra_env or {})},
    )


def _docker_compose_with_override(override_path: str, *args: str) -> subprocess.CompletedProcess:
    """Führt `docker compose -f docker-compose.yml -f <override> …` aus."""
    if shutil.which("docker") is None:
        pytest.skip("Docker ist in dieser Umgebung nicht verfügbar.")

    if subprocess.run(["docker", "info"], capture_output=True, check=False).returncode != 0:
        pytest.skip("Docker-Daemon ist in dieser Umgebung nicht verfügbar.")

    return subprocess.run(
        ["docker", "compose", "-f", "docker-compose.yml", "-f", override_path, *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def _wait_for_service_state(
    service: str,
    expected_state: str = "running",
    timeout: int = 90,
    override_path: Optional[str] = None,
) -> None:
    """Wartet, bis der Container-Status eines Services den Erwartungswert enthält."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = (
            _docker_compose_with_override(override_path, "ps", "--format", "json")
            if override_path
            else _docker_compose("ps", "--format", "json")
        )
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
                if expected_state in state or (expected_state == "running" and "running" in state):
                    return
                if health and expected_state in health:
                    return
        time.sleep(2)

    raise AssertionError(
        f"Service {service!r} nicht innerhalb von {timeout}s auf {expected_state!r} gesetzt."
    )


def _get_worker_container_name(service: str = "worker-wait",
                               override_path: Optional[str] = None) -> Optional[str]:
    """
    Ermittelt den echten Container-Namen des angegebenen Compose-Services
    (worker-Dienste haben keinen fixen container_name).
    Gibt None zurück wenn nicht gefunden.
    """
    result = (
        _docker_compose_with_override(override_path, "ps", "--format", "json")
        if override_path
        else _docker_compose("ps", "--format", "json")
    )
    if result.returncode != 0:
        return None

    for raw in result.stdout.splitlines():
        if not raw.strip():
            continue
        try:
            entry = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if entry.get("Service") == service:
            name = entry.get("Name", "")
            if name:
                return name
    return None


def _bring_up(override_path: str) -> None:
    """Fährt System mit Override hoch und wartet auf alle Services + Heartbeat-Pause."""
    up = _docker_compose_with_override(
        override_path, "up", "-d", "--build", "--remove-orphans",
    )
    assert up.returncode == 0, (
        f"docker compose up fehlgeschlagen:\n{up.stderr or up.stdout}"
    )
    for service in ("namensdienst", "dispatcher", "worker-wait"):
        _wait_for_service_state(service, timeout=180, override_path=override_path)
    # Worker sendet alle 5s Heartbeat → erst nach erstem Heartbeat im Namensdienst registriert
    time.sleep(6)


def _bring_down(override_path: str) -> None:
    _docker_compose_with_override(override_path, "down", "-v", "--remove-orphans")


def _kill_container(container_name: str) -> None:
    """
    Sendet SIGKILL an den Container (entspricht `docker kill <name>`).
    Kein graceful shutdown — kein SIGTERM, kein DEREGISTER.
    """
    result = subprocess.run(
        ["docker", "kill", container_name],
        capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, (
        f"docker kill {container_name!r} fehlgeschlagen:\n"
        f"{result.stderr or result.stdout}"
    )


# ── gRPC-Hilfsfunktionen ──────────────────────────────────────────────────────

def _post_task(task_type: str, payload: str, request_id: str) -> int:
    with grpc.insecure_channel(DISPATCHER_ADDRESS) as channel:
        stub = taskgrid_pb2_grpc.DispatcherServiceStub(channel)
        response = stub.PostTask(
            taskgrid_pb2.PostTaskRequest(
                message_type="POST_TASK",
                request_id=request_id,
                timestamp=int(time.time()),
                sender="pytest-worker-crash-test",
                payload=taskgrid_pb2.PostTaskRequest.Payload(
                    task_type=task_type,
                    task_payload=payload,
                ),
            )
        )
    assert response.payload.success, response.payload.message
    return int(response.payload.task_id)


def _get_result(task_id: int) -> taskgrid_pb2.ResultResponse:
    with grpc.insecure_channel(DISPATCHER_ADDRESS) as channel:
        stub = taskgrid_pb2_grpc.DispatcherServiceStub(channel)
        return stub.GetResult(
            taskgrid_pb2.GetResultRequest(
                message_type="GET_RESULT",
                request_id=f"result-{task_id}",
                timestamp=int(time.time()),
                sender="pytest-worker-crash-test",
                payload=taskgrid_pb2.GetResultRequest.Payload(task_id=task_id),
            )
        )


def _poll_until_terminal(task_id: int, timeout: int) -> Optional[taskgrid_pb2.ResultResponse]:
    """Pollt bis COMPLETED oder FAILED; gibt None zurück wenn Budget ausläuft."""
    deadline = time.time() + timeout
    result   = None
    while time.time() < deadline:
        result = _get_result(task_id)
        if result.payload.status in {"COMPLETED", "FAILED"}:
            return result
        time.sleep(1)
    return result


def _poll_for_any_of(
    task_id: int, statuses: set, timeout: int
) -> Optional[taskgrid_pb2.ResultResponse]:
    """Pollt bis einer der erwarteten Status erreicht wird; None bei Timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = _get_result(task_id)
        if result.payload.status in statuses:
            return result
        time.sleep(0.5)
    return None


def _get_dispatcher_logs() -> str:
    result = subprocess.run(
        ["docker", "logs", "dispatcher"],
        capture_output=True, text=True, check=False,
    )
    return result.stdout + result.stderr


def _lookup_worker_via_nameservice(task_type: str) -> list:
    """
    Fragt den Namensdienst direkt (localhost:50052) nach Workern für task_type.
    Gibt die Liste der zurückgegebenen WorkerPayload-Objekte zurück (kann leer sein).
    Wirft keine Exception bei Verbindungsfehlern — gibt leere Liste zurück.
    """
    try:
        with grpc.insecure_channel(NAMENSDIENST_ADDRESS) as channel:
            stub = taskgrid_pb2_grpc.NamingServiceStub(channel)
            response = stub.LookupWorker(
                taskgrid_pb2.LookupRequest(
                    message_type="LOOKUP_WORKER",
                    request_id="crash-test-lookup",
                    timestamp=int(time.time()),
                    sender="pytest-worker-crash-test",
                    payload=taskgrid_pb2.LookupRequest.Payload(task_type=task_type),
                ),
                timeout=5.0,
            )
        return list(response.payload.workers)
    except grpc.RpcError:
        return []


# ── Testfall 1: Abrupter Absturz → Task endet in FAILED ──────────────────────

def test_worker_crash_task_reaches_failed():
    """
    Kernszenario: Worker wird per docker kill abrupt gestoppt.
    Kein DEREGISTER, kein ReturnResult — der Dispatcher erhält niemals ein Ergebnis.

    Erwartetes Verhalten (für beide möglichen Pfade):
      Pfad A (Timer gewinnt):
        QUEUED → DISPATCHED → [Timer 3s] → TIMEOUT → RETRYING
               → DISPATCHED → [kein Worker mehr] → FAILED (oder 2. Timer)
      Pfad B (gRPC-Fehler gewinnt):
        QUEUED → DISPATCHED → [gRPC UNAVAILABLE] → FAILED direkt

    In jedem Fall: Task.status == FAILED, kein COMPLETED.
    Log: 'DISPATCH_TIMEOUT_fired' ODER 'DISPATCH_worker_unreachable' (mind. eines).
    """
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", suffix=".yml",
        prefix="taskgrid_worker_crash_", delete=False,
    ) as f:
        f.write(OVERRIDE_CONTENT)
        override_path = f.name

    try:
        _bring_up(override_path)
        try:
            # ── Container-Namen ermitteln (vor dem Kill) ───────────────────
            container_name = _get_worker_container_name("worker-wait", override_path)
            assert container_name is not None, (
                "Container-Name für worker-wait nicht gefunden. "
                "Ist der Service gestartet?"
            )

            # ── Task senden: wait=30s ──────────────────────────────────────
            task_id = _post_task(
                "wait", str(WAIT_TASK_DURATION_SECS), "worker-crash-main"
            )

            # ── Warten bis Task DISPATCHED ist ────────────────────────────
            dispatched = _poll_for_any_of(
                task_id, {"DISPATCHED", "TIMEOUT", "RETRYING", "FAILED"},
                timeout=10,
            )
            assert dispatched is not None, (
                f"Task {task_id} hat DISPATCHED nicht innerhalb von 10s erreicht. "
                f"Dispatch-Loop läuft möglicherweise nicht. "
                f"Letzter Status: {_get_result(task_id).payload.status}"
            )

            # ── Worker abrupt killen ───────────────────────────────────────
            # Kurz warten damit der gRPC-Call sicher im Worker angenommen wurde
            # (Worker-Prozess hat ExecuteTask() betreten), dann SIGKILL.
            time.sleep(WORKER_KILL_DELAY_SECS)
            _kill_container(container_name)

            # ── Warten bis FAILED ──────────────────────────────────────────
            terminal = _poll_until_terminal(task_id, timeout=TERMINAL_POLL_TIMEOUT_SECS)

            assert terminal is not None, (
                f"Task {task_id} hat keinen terminalen Status innerhalb von "
                f"{TERMINAL_POLL_TIMEOUT_SECS}s erreicht. "
                f"Letzter Status: {_get_result(task_id).payload.status}"
            )
            assert terminal.payload.found, f"Task {task_id} nicht im Dispatcher gefunden."
            assert terminal.payload.status == "FAILED", (
                f"Erwartet: FAILED nach Worker-Absturz, "
                f"tatsächlich: '{terminal.payload.status}'. "
                f"Der tote Worker hat offenbar ein Ergebnis geschickt — "
                f"das ist bei SIGKILL nicht möglich."
            )

            # ── Log-Prüfung: mind. ein Fehler-Event vorhanden ─────────────
            logs = _get_dispatcher_logs()

            has_timeout_event   = "DISPATCH_TIMEOUT_fired"    in logs
            has_unreachable     = "DISPATCH_worker_unreachable" in logs
            has_stale_ignored   = "DISPATCH_stale_rpc_error_ignored" in logs

            assert has_timeout_event or has_unreachable or has_stale_ignored, (
                f"Kein erwartetes Fehler-Event im Dispatcher-Log. "
                f"Weder 'DISPATCH_TIMEOUT_fired' noch 'DISPATCH_worker_unreachable' "
                f"noch 'DISPATCH_stale_rpc_error_ignored' gefunden.\n"
                f"Dispatcher-Logs (letzte 3000 Zeichen):\n{logs[-3000:]}"
            )
            assert f"task_id={task_id}" in logs, (
                f"task_id={task_id} fehlt im Dispatcher-Log."
            )

        finally:
            _bring_down(override_path)
    finally:
        try:
            os.unlink(override_path)
        except OSError:
            pass


# ── Testfall 2: Namensdienst markiert Worker nach ausgebliebenen Heartbeats OFFLINE

def test_worker_crash_nameservice_marks_offline():
    """
    Prüft, dass der Namensdienst den abgestürzten Worker nach ausgebliebenen
    Heartbeats als OFFLINE markiert und aus seiner Registrierung entfernt.

    Heartbeat-Intervall des Workers: 5s.
    Namensdienst entfernt Worker nach 10s ohne Heartbeat (startLoop(5, 2): 5s * 2).

    Ablauf:
      1. System hochfahren, Worker verifizieren (im Namensdienst registriert).
      2. Worker abrupt killen.
      3. Warten bis NAMESERVICE_OFFLINE_SECS + Puffer.
      4. LookupWorker(task_type='wait') → muss leer zurückgeben (Worker entfernt).
    """
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", suffix=".yml",
        prefix="taskgrid_crash_offline_", delete=False,
    ) as f:
        f.write(OVERRIDE_CONTENT)
        override_path = f.name

    try:
        _bring_up(override_path)
        try:
            # ── 1. Vorbedingung: Worker ist im Namensdienst registriert ───
            workers_before = _lookup_worker_via_nameservice("wait")
            assert len(workers_before) > 0, (
                "Vorbedingung nicht erfüllt: Kein 'wait'-Worker im Namensdienst "
                "registriert. Ist der worker-wait-Container gestartet und hat "
                "mindestens einen Heartbeat geschickt?"
            )

            # ── 2. Container-Namen ermitteln und Worker killen ─────────────
            container_name = _get_worker_container_name("worker-wait", override_path)
            assert container_name is not None, (
                "Container-Name für worker-wait nicht gefunden."
            )
            _kill_container(container_name)

            # ── 3. Auf OFFLINE warten (polling) ───────────────────────────
            # Namensdienst entfernt Worker nach ~10s ohne Heartbeat.
            # Wir pollen bis keine 'wait'-Worker mehr gefunden werden.
            deadline = time.time() + NAMESERVICE_OFFLINE_POLL_SECS
            workers_after = _lookup_worker_via_nameservice("wait")
            while time.time() < deadline and len(workers_after) > 0:
                time.sleep(2)
                workers_after = _lookup_worker_via_nameservice("wait")

            # ── 4. Prüfen: keine aktiven 'wait'-Worker mehr ────────────────
            assert len(workers_after) == 0, (
                f"Namensdienst gibt nach {NAMESERVICE_OFFLINE_POLL_SECS}s noch "
                f"{len(workers_after)} 'wait'-Worker zurück, obwohl der Container "
                f"per SIGKILL beendet wurde. "
                f"Registrierte Worker: {[w.worker_id for w in workers_after]}. "
                f"Heartbeat-Timeout (OFFLINE) ist {NAMESERVICE_OFFLINE_SECS}s — "
                f"der Namensdienst sollte den Worker bereits entfernt haben."
            )

        finally:
            _bring_down(override_path)
    finally:
        try:
            os.unlink(override_path)
        except OSError:
            pass


# ── Testfall 3: System bleibt nach Worker-Crash stabil ───────────────────────

def test_worker_crash_system_remains_stable():
    """
    Stabilitätsnachweis: Nach dem abrupten Absturz des worker-wait-Containers
    verarbeitet der Dispatcher weiterhin Tasks anderer Worker-Typen korrekt.

    Szenario:
      1. System hochfahren (alle Worker, inkl. worker-sum).
      2. wait-Task senden, Worker abrupt killen.
      3. Task endet in FAILED (Dispatcher hat Absturz erkannt).
      4. Neuen sum-Task senden → muss COMPLETED werden.

    Dies stellt sicher, dass der Absturz eines Workers keine globale Störung
    im Dispatcher verursacht (kein Deadlock, kein Zustandsmüll, keine Exception).
    """
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", suffix=".yml",
        prefix="taskgrid_crash_stable_", delete=False,
    ) as f:
        f.write(OVERRIDE_CONTENT)
        override_path = f.name

    try:
        _bring_up(override_path)

        # Sicherstellen, dass auch worker-sum läuft
        _wait_for_service_state("worker-sum", timeout=90, override_path=override_path)

        try:
            # ── 1. wait-Task senden und Worker killen ──────────────────────
            container_name = _get_worker_container_name("worker-wait", override_path)
            assert container_name is not None, (
                "Container-Name für worker-wait nicht gefunden."
            )

            crash_task_id = _post_task(
                "wait", str(WAIT_TASK_DURATION_SECS), "worker-crash-stability-wait"
            )

            # Warten bis Task dispatcht wurde, dann killen
            crash_dispatched = _poll_for_any_of(
                crash_task_id, {"DISPATCHED", "TIMEOUT", "RETRYING", "FAILED"},
                timeout=10,
            )
            assert crash_dispatched is not None, (
                f"Task {crash_task_id} hat DISPATCHED nicht erreicht."
            )

            time.sleep(WORKER_KILL_DELAY_SECS)
            _kill_container(container_name)

            # ── 2. Auf FAILED warten ───────────────────────────────────────
            crash_terminal = _poll_until_terminal(
                crash_task_id, timeout=TERMINAL_POLL_TIMEOUT_SECS
            )
            assert crash_terminal is not None, (
                f"Crash-Task {crash_task_id} hat keinen terminalen Status erreicht."
            )
            assert crash_terminal.payload.status == "FAILED", (
                f"Crash-Task sollte FAILED sein, ist aber: "
                f"'{crash_terminal.payload.status}'."
            )

            # ── 3. Stabilitäts-Task: sum (anderer Worker-Typ, lebt noch) ──
            stable_id = _post_task("sum", "3,7", "worker-crash-stability-sum")
            stable    = _poll_until_terminal(stable_id, timeout=STABILITY_POLL_TIMEOUT_SECS)

            assert stable is not None, (
                f"Stabilitäts-Task {stable_id} (sum) hat kein Ergebnis geliefert. "
                f"Dispatcher möglicherweise instabil nach Worker-Absturz."
            )
            assert stable.payload.status == "COMPLETED", (
                f"Stabilitäts-Task (sum=3+7) sollte COMPLETED sein, "
                f"tatsächlich: '{stable.payload.status}'. "
                f"Dispatcher nach Worker-Absturz instabil."
            )
            assert stable.payload.result == "10", (
                f"Falsches Ergebnis: erwartet '10', erhalten '{stable.payload.result}'."
            )

        finally:
            _bring_down(override_path)
    finally:
        try:
            os.unlink(override_path)
        except OSError:
            pass


# ── Testfall 4: Kein COMPLETED nach Worker-Absturz ───────────────────────────

def test_worker_crash_never_completed():
    """
    Negativtest: Ein per SIGKILL gestoppter Worker kann physisch kein
    ReturnResult senden. Der Task darf daher niemals COMPLETED werden.

    Prüft, dass der terminale Status ausschließlich FAILED ist.
    Schlägt fehl wenn der Dispatcher fälschlicherweise COMPLETED setzt
    (z.B. durch Race-Condition in der Zustandsmaschine).
    """
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", suffix=".yml",
        prefix="taskgrid_crash_no_complete_", delete=False,
    ) as f:
        f.write(OVERRIDE_CONTENT)
        override_path = f.name

    try:
        _bring_up(override_path)
        try:
            container_name = _get_worker_container_name("worker-wait", override_path)
            assert container_name is not None, (
                "Container-Name für worker-wait nicht gefunden."
            )

            task_id = _post_task(
                "wait", str(WAIT_TASK_DURATION_SECS), "worker-crash-no-complete"
            )

            # Kurz warten bis DISPATCHED, dann sofort killen
            _poll_for_any_of(
                task_id, {"DISPATCHED", "TIMEOUT", "RETRYING", "FAILED"},
                timeout=10,
            )
            time.sleep(WORKER_KILL_DELAY_SECS)
            _kill_container(container_name)

            terminal = _poll_until_terminal(task_id, timeout=TERMINAL_POLL_TIMEOUT_SECS)

            assert terminal is not None, (
                f"Task {task_id} hat keinen terminalen Status erreicht."
            )
            assert terminal.payload.status != "COMPLETED", (
                f"Task {task_id} wurde COMPLETED obwohl der Worker per SIGKILL "
                f"getötet wurde und niemals ReturnResult senden konnte. "
                f"Dies deutet auf eine schwerwiegende Race-Condition hin."
            )
            assert terminal.payload.status == "FAILED", (
                f"Unerwarteter terminaler Status: '{terminal.payload.status}'. "
                f"Erwartet: 'FAILED'."
            )

        finally:
            _bring_down(override_path)
    finally:
        try:
            os.unlink(override_path)
        except OSError:
            pass
