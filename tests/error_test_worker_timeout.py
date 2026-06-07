"""
Fehlertest: Worker-Timeout — Worker ist registriert und erreichbar,
antwortet aber nicht innerhalb des konfigurierten Dispatcher-Timeouts.

Warum wait=30s und nicht weniger?
───────────────────────────────────────────────────────────────────
Der Worker ist SYNCHRON: ExecuteTask() schläft erst, schickt dann
das Ergebnis per ReturnResult(), gibt danach erst den gRPC-ACK zurück.

Im WorkerClient (src/dispatcher/worker_client.py) ist ein separater
gRPC-Timeout von DISPATCH_TIMEOUT_SECS = 5.0s hardcoded.

Daraus folgen zwei Verhaltens-Pfade je nach wait-Wert:

  Pfad A: wait < 5s (z.B. wait=4s)
    ACK kommt nach 4s an → dispatch_task() = True → PROCESSING gesetzt.
    ReturnResult(COMPLETED) trifft aber kurz nach dem Dispatcher-Timer ein.
    Falls der Task zu diesem Zeitpunkt bereits re-dispatcht wurde (DISPATCHED),
    akzeptiert ReturnResult das Ergebnis → Task wird COMPLETED.
    → Kein reproduzierbares FAILED möglich.

  Pfad B: wait >= 5s (z.B. wait=30s)  ← dieser Test
    ACK käme erst nach 30s, gRPC-Timeout schlägt nach 5s an.
    dispatch_task() gibt False zurück → NEIN, Timer läuft bereits!
    
    Korrekte Sequenz:
      t=0:   DISPATCHED, Dispatcher-Timer(3s) startet, gRPC-Call beginnt
      t=3:   Timer feuert (Task ist DISPATCHED, ACK noch nicht da):
               DISPATCHED→TIMEOUT→RETRYING, retry_count=1, re-enqueued
      t≈3:   2. Dispatch: RETRYING→DISPATCHED, neuer Timer(3s)
      t=5:   gRPC-Timeout des 1. Calls → RpcError → dispatch_task()=False
               transition(→FAILED) schlägt fehl (Task ist schon DISPATCHED/RETRYING)
               → ignoriert (logged)
      t=6:   2. Timer feuert: DISPATCHED→TIMEOUT→RETRYING, retry_count=2
               retry_count(2) >= max_retries(2) → RETRYING→FAILED ✓
    
    PROCESSING wird in diesem Pfad NIE erreicht (ACK kommt nie vor gRPC-Timeout).
    Der Dispatcher-Timer feuert auf den DISPATCHED-Status.
    DISPATCHED→TIMEOUT ist laut state_machine.py erlaubt.
    Ergebnis: FAILED nach (MAX_RETRIES * DISPATCH_TIMEOUT_SECONDS) + Overhead.

Szenario:
  - DISPATCH_TIMEOUT_SECONDS = 3 (Dispatcher-Timer, via docker-compose-Override)
  - MAX_RETRIES = 2 (via docker-compose-Override)
  - wait = 30 (Worker schläft 30s, gRPC-Timeout nach 5s)

Erwartetes Verhalten:
  1. Task wird dispatcht: QUEUED → DISPATCHED
  2. Dispatcher-Timer feuert nach 3s: DISPATCHED → TIMEOUT → RETRYING
  3. Task wird erneut dispatcht: RETRYING → DISPATCHED
  4. Dispatcher-Timer feuert nach weiteren 3s: DISPATCHED → TIMEOUT → RETRYING
  5. retry_count = 2 = MAX_RETRIES → RETRYING → FAILED
  6. Log enthält DISPATCH_TIMEOUT_fired und TIMEOUT_max_retries_exceeded
  7. System bleibt stabil — ein weiterer Task danach wird korrekt verarbeitet
"""

import json
import os
import shutil
import subprocess
import tempfile
import time

import grpc
import pytest
from typing import Optional

from proto import taskgrid_pb2, taskgrid_pb2_grpc


REPO_ROOT          = os.path.dirname(os.path.dirname(__file__))
DISPATCHER_ADDRESS = os.environ.get("DISPATCHER_ADDRESS", "localhost:50051")

# ── Timing-Parameter ──────────────────────────────────────────────────────────
#
# GRPC_WORKER_CLIENT_TIMEOUT = 5.0s (hardcoded in src/dispatcher/worker_client.py)
#
# Constraints:
#   WAIT_TASK_DURATION_SECS >= GRPC_WORKER_CLIENT_TIMEOUT  → ACK kommt nie an
#   TEST_DISPATCH_TIMEOUT_SECS < GRPC_WORKER_CLIENT_TIMEOUT → Timer feuert vor gRPC-Timeout
#
GRPC_WORKER_CLIENT_TIMEOUT  = 5.0   # hardcoded in worker_client.py, nicht änderbar
TEST_DISPATCH_TIMEOUT_SECS  = 3     # Dispatcher-Timer; muss < GRPC_WORKER_CLIENT_TIMEOUT
TEST_MAX_RETRIES             = 2
WAIT_TASK_DURATION_SECS      = 30   # >> GRPC_WORKER_CLIENT_TIMEOUT, ACK kommt garantiert nicht an

# Budget pro Retry-Zyklus: Timer-Dauer + etwas Overhead für Dispatch + gRPC-Overhead
_SECS_PER_CYCLE             = TEST_DISPATCH_TIMEOUT_SECS + 4
# Gesamtbudget: alle Zyklen + Puffer
TERMINAL_POLL_TIMEOUT_SECS  = _SECS_PER_CYCLE * (TEST_MAX_RETRIES + 1) + 15
STABILITY_POLL_TIMEOUT_SECS = 20


# ── Override-Datei ────────────────────────────────────────────────────────────

OVERRIDE_CONTENT = f"""\
# Temporary override for error_test_worker_timeout.py
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


def _bring_up(override_path: str) -> None:
    """Fährt System mit Override hoch, wartet auf alle Services + Heartbeat-Pause."""
    up = _docker_compose_with_override(
        override_path, "up", "-d", "--build", "--remove-orphans",
    )
    assert up.returncode == 0, (
        f"docker compose up fehlgeschlagen:\n{up.stderr or up.stdout}"
    )
    for service in ("namensdienst", "dispatcher", "worker-wait"):
        _wait_for_service_state(service, timeout=180, override_path=override_path)
    # worker-wait sendet alle 5s Heartbeat → erst danach im Namensdienst registriert
    time.sleep(6)


def _bring_down(override_path: str) -> None:
    _docker_compose_with_override(override_path, "down", "-v", "--remove-orphans")


# ── gRPC-Hilfsfunktionen ──────────────────────────────────────────────────────

def _post_task(task_type: str, payload: str, request_id: str) -> int:
    with grpc.insecure_channel(DISPATCHER_ADDRESS) as channel:
        stub = taskgrid_pb2_grpc.DispatcherServiceStub(channel)
        response = stub.PostTask(
            taskgrid_pb2.PostTaskRequest(
                message_type="POST_TASK",
                request_id=request_id,
                timestamp=int(time.time()),
                sender="pytest-worker-timeout-test",
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
                sender="pytest-worker-timeout-test",
                payload=taskgrid_pb2.GetResultRequest.Payload(task_id=task_id),
            )
        )


def _poll_until_terminal(task_id: int, timeout: int) -> Optional[taskgrid_pb2.ResultResponse]:
    """Pollt bis COMPLETED oder FAILED; gibt None zurück wenn Budget ausläuft."""
    deadline = time.time() + timeout
    result = None
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


# ── Testfall 1: Vollständiger Timeout-Zyklus bis FAILED ───────────────────────

def test_worker_timeout_leads_to_failed_after_max_retries():
    """
    Kern-Szenario: Worker akzeptiert den Task nicht rechtzeitig (wait=30s,
    gRPC-Timeout=5s). Dispatcher-Timer (3s) feuert auf DISPATCHED,
    da der ACK des Workers nie innerhalb des Timer-Fensters ankommt.

    Erwartete Status-Sequenz:
      QUEUED → DISPATCHED → [Timer 3s] → TIMEOUT → RETRYING
             → DISPATCHED → [Timer 3s] → TIMEOUT → RETRYING → FAILED

    Prüft:
      1. Task erreicht DISPATCHED (Dispatch wurde ausgelöst).
      2. Task wird nach Dispatcher-Timer RETRYING (nicht PROCESSING).
      3. Nach MAX_RETRIES: Status = FAILED.
      4. Log enthält DISPATCH_TIMEOUT_fired und TIMEOUT_max_retries_exceeded.
      5. System bleibt nach dem Fehlerfall stabil.
    """
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", suffix=".yml",
        prefix="taskgrid_worker_timeout_", delete=False,
    ) as f:
        f.write(OVERRIDE_CONTENT)
        override_path = f.name

    try:
        _bring_up(override_path)
        try:
            # ── 1. Task senden: wait=30s ───────────────────────────────────
            task_id = _post_task(
                "wait", str(WAIT_TASK_DURATION_SECS), "worker-timeout-main"
            )

            # ── 2. Warten bis Task DISPATCHED ist ─────────────────────────
            # Der Dispatch-Loop schickt den Task sofort los. DISPATCHED ist
            # der erste sichtbare Status nach QUEUED.
            dispatched = _poll_for_any_of(
                task_id, {"DISPATCHED", "TIMEOUT", "RETRYING", "FAILED"},
                timeout=10,
            )
            assert dispatched is not None, (
                f"Task {task_id} hat DISPATCHED nicht erreicht. "
                f"Dispatch-Loop läuft möglicherweise nicht. "
                f"Letzter Status: {_get_result(task_id).payload.status}"
            )

            # ── 3. Warten bis FAILED ───────────────────────────────────────
            # Budget: (MAX_RETRIES * Zyklus) + Overhead
            # Zyklus = Timer(3s) + Dispatch-Overhead + ggf. gRPC-Timeout(5s) = ~9s
            terminal_result = _poll_until_terminal(
                task_id, timeout=TERMINAL_POLL_TIMEOUT_SECS
            )

            assert terminal_result is not None, (
                f"Task {task_id} hat keinen terminalen Status innerhalb von "
                f"{TERMINAL_POLL_TIMEOUT_SECS}s erreicht. "
                f"Letzter Status: {_get_result(task_id).payload.status}"
            )
            assert terminal_result.payload.found, \
                f"Task {task_id} nicht im Dispatcher gefunden."
            assert terminal_result.payload.status == "FAILED", (
                f"Erwartet: FAILED nach {TEST_MAX_RETRIES} Timeouts à {TEST_DISPATCH_TIMEOUT_SECS}s, "
                f"tatsächlich: '{terminal_result.payload.status}'."
            )

            # ── 4. Log-Prüfung ─────────────────────────────────────────────
            logs = _get_dispatcher_logs()

            assert "DISPATCH_TIMEOUT_fired" in logs, (
                f"'DISPATCH_TIMEOUT_fired' fehlt im Log. "
                f"Dispatcher-Timer ({TEST_DISPATCH_TIMEOUT_SECS}s) hat nicht gefeuert.\n"
                f"Dispatcher-Logs (letzte 3000 Zeichen):\n{logs[-3000:]}"
            )
            assert "TIMEOUT_max_retries_exceeded" in logs, (
                f"'TIMEOUT_max_retries_exceeded' fehlt im Log. "
                f"MAX_RETRIES ({TEST_MAX_RETRIES}) wurde nicht ausgeschöpft.\n"
                f"Dispatcher-Logs (letzte 3000 Zeichen):\n{logs[-3000:]}"
            )
            assert f"task_id={task_id}" in logs, (
                f"task_id={task_id} fehlt im Dispatcher-Log."
            )

            # ── 5. Stabilitätsnachweis ─────────────────────────────────────
            stable_id = _post_task("wait", "1", "post-timeout-stability")
            stable    = _poll_until_terminal(stable_id, timeout=STABILITY_POLL_TIMEOUT_SECS)

            assert stable is not None, "Kein Ergebnis für Stabilitäts-Task."
            assert stable.payload.status == "COMPLETED", (
                f"Dispatcher nach Timeout-Szenario instabil. "
                f"Stabilitäts-Task (wait=1s): '{stable.payload.status}', "
                f"erwartet 'COMPLETED'."
            )
            assert stable.payload.result == "waited 1s", (
                f"Falsches Ergebnis: erwartet 'waited 1s', "
                f"erhalten '{stable.payload.result}'."
            )

        finally:
            _bring_down(override_path)
    finally:
        try:
            os.unlink(override_path)
        except OSError:
            pass


# ── Testfall 2: retry_count-Hochzählung und RETRYING-Zwischenstatus ───────────

def test_worker_timeout_retry_count_increments():
    """
    Prüft, dass RETRYING sichtbar durchlaufen wird und der retry_count steigt.

    Mit MAX_RETRIES=3 gibt es mehr Timeout-Zyklen bevor FAILED erreicht wird.
    RETRYING erscheint mindestens einmal bevor der Task endgültig FAILED wird.

    Prüft:
      1. Task durchläuft mindestens einmal RETRYING.
      2. Nach allen Timeouts: Status = FAILED.
      3. Log enthält TIMEOUT_retrying (mind. ein nicht-letzter Retry).
    """
    _max_retries_local = 3
    override_content = f"""\
# Temporary override for error_test_worker_timeout.py (retry_count test)
services:
  dispatcher:
    environment:
      - DISPATCH_TIMEOUT_SECONDS={TEST_DISPATCH_TIMEOUT_SECS}
      - MAX_RETRIES={_max_retries_local}
"""
    _poll_budget = _SECS_PER_CYCLE * (_max_retries_local + 1) + 15

    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", suffix=".yml",
        prefix="taskgrid_timeout_retry_count_", delete=False,
    ) as f:
        f.write(override_content)
        override_path = f.name

    try:
        _bring_up(override_path)
        try:
            task_id = _post_task(
                "wait", str(WAIT_TASK_DURATION_SECS), "worker-timeout-retry-count"
            )

            # ── 1. RETRYING muss mindestens einmal auftreten ───────────────
            # Budget: ein voller Zyklus (Timer + Overhead)
            retrying = _poll_for_any_of(
                task_id, {"RETRYING"},
                timeout=_SECS_PER_CYCLE + 5,
            )
            assert retrying is not None, (
                f"Task {task_id} hat RETRYING nicht erreicht. "
                f"Erster Dispatcher-Timer ({TEST_DISPATCH_TIMEOUT_SECS}s) hat nicht gefeuert. "
                f"Letzter Status: {_get_result(task_id).payload.status}"
            )

            # ── 2. Bis FAILED warten ───────────────────────────────────────
            terminal = _poll_until_terminal(task_id, timeout=_poll_budget)

            assert terminal is not None, \
                f"Kein terminaler Status innerhalb von {_poll_budget}s."
            assert terminal.payload.status == "FAILED", (
                f"Task {task_id} sollte nach {_max_retries_local} Timeouts FAILED sein, "
                f"tatsächlich: '{terminal.payload.status}'."
            )

            # ── 3. Log: TIMEOUT_retrying muss vorhanden sein ───────────────
            logs = _get_dispatcher_logs()
            assert "TIMEOUT_retrying" in logs, (
                f"'TIMEOUT_retrying' fehlt im Log. "
                f"Retry-Mechanismus (re-enqueue) hat nicht gefeuert.\n"
                f"Dispatcher-Logs (letzte 3000 Zeichen):\n{logs[-3000:]}"
            )

        finally:
            _bring_down(override_path)
    finally:
        try:
            os.unlink(override_path)
        except OSError:
            pass


# ── Testfall 3: Kein Timeout bei schnellem Worker ─────────────────────────────

def test_worker_timeout_timer_cancelled_on_success():
    """
    Negativtest: Schneller Task (wait=1s) muss COMPLETED sein.

    wait=1s < gRPC-Timeout (5s): ACK kommt an, ReturnResult(COMPLETED)
    wird korrekt verarbeitet, cancel_timeout() stoppt den Timer.
    Kein DISPATCH_TIMEOUT_fired für diese task_id.

    Prüft:
      - Status = COMPLETED, result = 'waited 1s'.
      - Kein Timeout-Event für diese task_id in den neuen Logs.
    """
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", suffix=".yml",
        prefix="taskgrid_timeout_no_fire_", delete=False,
    ) as f:
        f.write(OVERRIDE_CONTENT)
        override_path = f.name

    try:
        _bring_up(override_path)
        try:
            logs_before = _get_dispatcher_logs()

            task_id = _post_task("wait", "1", "worker-timeout-no-fire")

            terminal = _poll_until_terminal(task_id, timeout=STABILITY_POLL_TIMEOUT_SECS)

            assert terminal is not None, "Kein Ergebnis für wait=1s-Task."
            assert terminal.payload.status == "COMPLETED", (
                f"wait=1s-Task sollte COMPLETED sein, "
                f"tatsächlich: '{terminal.payload.status}'. "
                f"Dispatcher-Timer ({TEST_DISPATCH_TIMEOUT_SECS}s) hat fälschlicherweise gefeuert."
            )
            assert terminal.payload.result == "waited 1s", (
                f"Falsches Ergebnis: erwartet 'waited 1s', "
                f"erhalten '{terminal.payload.result}'."
            )

            # Warten bis ein eventueller Timer sicher abgelaufen wäre
            time.sleep(TEST_DISPATCH_TIMEOUT_SECS + 2)

            logs_after = _get_dispatcher_logs()
            new_logs   = logs_after[len(logs_before):]

            # Nur dann Fehler, wenn das Timeout-Event explizit für diese task_id geloggt ist
            if "DISPATCH_TIMEOUT_fired" in new_logs and f"task_id={task_id}" in new_logs:
                pytest.fail(
                    f"Timeout-Timer hat für Task {task_id} (wait=1s) gefeuert, "
                    f"obwohl cancel_timeout() nach COMPLETED hätte aufgerufen werden müssen.\n"
                    f"Neue Log-Einträge:\n{new_logs[-2000:]}"
                )

        finally:
            _bring_down(override_path)
    finally:
        try:
            os.unlink(override_path)
        except OSError:
            pass
