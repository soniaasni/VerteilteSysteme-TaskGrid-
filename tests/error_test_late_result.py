"""
Fehlertest: Verspätetes Worker-Ergebnis nach Dispatcher-Timeout.

Szenario:
  - Dispatcher-Timeout sehr niedrig (3 Sekunden).
  - Worker-wait-Task mit 10 Sekunden künstlicher Verzögerung.

Erwartetes Verhalten (End-to-End):
  1. Client sendet send_task("wait", "10")
  2. Dispatcher dispatcht Task zu worker-wait.
  3. Nach 3 s feuert der Timeout-Timer:
       DISPATCHED/PROCESSING → TIMEOUT → RETRYING → (MAX_RETRIES=1) → FAILED
  4. Nach 10 s meldet worker-wait sein Ergebnis per RESULT_RETURN.
  5. Dispatcher erkennt is_terminal(task) == True (Status = FAILED):
       → Ergebnis wird IGNORIERT, Status NICHT überschrieben.
       → Log-Eintrag: event=RESULT_RETURN_late ... status=FAILED
  6. GetResult liefert status=FAILED, result bleibt leer (kein Überschreiben).
  7. System bleibt stabil – kein Absturz, weiterer Task danach verarbeitbar.

Umsetzungsdetails:
  - Temporäre docker-compose-Override-Datei setzt DISPATCH_TIMEOUT_SECONDS=3
    und MAX_RETRIES=1 für den Dispatcher-Service (nur für diesen Test).
  - Log-Prüfung via `docker logs dispatcher` nach dem Szenario.
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

# Timeout und Retry-Einstellungen für diesen Test
TEST_DISPATCH_TIMEOUT_SECS = 3    # Dispatcher feuert nach 3 s
TEST_MAX_RETRIES           = 1    # Sofort FAILED nach erstem Timeout (kein echtes Retry)
WAIT_TASK_DURATION_SECS    = 10   # worker-wait schläft 10 s → kommt nach Timeout zurück

# Wie lange wir maximal auf einen terminalen Task-Status warten
TERMINAL_POLL_TIMEOUT_SECS = 30
# Wie lange wir nach dem terminalen Status noch warten,
# damit der Worker sein verspätetes Ergebnis senden kann
LATE_RESULT_GRACE_SECS     = WAIT_TASK_DURATION_SECS + 5  # 15 s Puffer


# ── Override-Datei ────────────────────────────────────────────────────────────

OVERRIDE_CONTENT = f"""\
# Temporary override for error_test_late_result.py
# Sets aggressive timeout and minimal retries on the dispatcher.
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

    daemon_check = subprocess.run(
        ["docker", "info"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if daemon_check.returncode != 0:
        pytest.skip("Docker-Daemon ist in dieser Umgebung nicht verfügbar.")

    cmd = ["docker", "compose", *args]
    env = {**os.environ, **(extra_env or {})}
    return subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )


def _docker_compose_with_override(override_path: str, *args: str) -> subprocess.CompletedProcess:
    """Führt `docker compose -f docker-compose.yml -f <override> …` aus."""
    if shutil.which("docker") is None:
        pytest.skip("Docker ist in dieser Umgebung nicht verfügbar.")

    daemon_check = subprocess.run(
        ["docker", "info"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if daemon_check.returncode != 0:
        pytest.skip("Docker-Daemon ist in dieser Umgebung nicht verfügbar.")

    cmd = [
        "docker", "compose",
        "-f", "docker-compose.yml",
        "-f", override_path,
        *args,
    ]
    return subprocess.run(
        cmd,
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
        if override_path:
            result = _docker_compose_with_override(override_path, "ps", "--format", "json")
        else:
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
                if expected_state in state or (expected_state == "running" and "running" in state):
                    return
                if health and expected_state in health:
                    return
        time.sleep(2)

    raise AssertionError(
        f"Service {service!r} wurde nicht innerhalb von {timeout}s "
        f"auf {expected_state!r} gesetzt."
    )


# ── gRPC-Hilfsfunktionen ──────────────────────────────────────────────────────

def _post_task(task_type: str, payload: str, request_id: str) -> int:
    """Sendet einen Task an den Dispatcher und gibt die task_id zurück."""
    with grpc.insecure_channel(DISPATCHER_ADDRESS) as channel:
        stub = taskgrid_pb2_grpc.DispatcherServiceStub(channel)
        response = stub.PostTask(
            taskgrid_pb2.PostTaskRequest(
                message_type="POST_TASK",
                request_id=request_id,
                timestamp=int(time.time()),
                sender="pytest-timeout-test",
                payload=taskgrid_pb2.PostTaskRequest.Payload(
                    task_type=task_type,
                    task_payload=payload,
                ),
            )
        )
    assert response.payload.success, response.payload.message
    return int(response.payload.task_id)


def _get_result(task_id: int) -> taskgrid_pb2.ResultResponse:
    """Fragt das Ergebnis eines Tasks beim Dispatcher ab."""
    with grpc.insecure_channel(DISPATCHER_ADDRESS) as channel:
        stub = taskgrid_pb2_grpc.DispatcherServiceStub(channel)
        return stub.GetResult(
            taskgrid_pb2.GetResultRequest(
                message_type="GET_RESULT",
                request_id=f"result-{task_id}",
                timestamp=int(time.time()),
                sender="pytest-timeout-test",
                payload=taskgrid_pb2.GetResultRequest.Payload(task_id=task_id),
            )
        )


def _poll_until_terminal(task_id: int, timeout: int) -> taskgrid_pb2.ResultResponse:
    """
    Pollt den Task-Status bis ein terminaler Zustand (COMPLETED, FAILED, TIMEOUT)
    erreicht wird oder der Timeout abläuft.
    """
    terminal_states = {"COMPLETED", "FAILED", "TIMEOUT"}
    deadline = time.time() + timeout
    result = None
    while time.time() < deadline:
        result = _get_result(task_id)
        if result.payload.status in terminal_states:
            return result
        time.sleep(1)
    return result  # letzten bekannten Stand zurückgeben


def _get_dispatcher_logs() -> str:
    """Liest die Logs des Dispatcher-Containers aus."""
    result = subprocess.run(
        ["docker", "logs", "dispatcher"],
        capture_output=True,
        text=True,
        check=False,
    )
    # docker logs schreibt auf stderr (Zeitstempel auf stdout, Inhalt auf stderr)
    return result.stdout + result.stderr


# ── Testfall ──────────────────────────────────────────────────────────────────

def test_late_result_is_ignored_after_timeout():
    """
    Timeout-Szenario: Worker liefert Ergebnis NACH Dispatcher-Timeout.

    Prüft:
      1. Task wechselt innerhalb von DISPATCH_TIMEOUT_SECONDS in Status FAILED.
      2. Verspätetes RESULT_RETURN des Workers wird vollständig ignoriert:
           - Status bleibt FAILED (kein Überschreiben auf COMPLETED).
           - task.result bleibt leer (kein Ergebnis eingetragen).
      3. Dispatcher-Log enthält den Eintrag event=RESULT_RETURN_late mit
         korrekter task_id.
      4. System bleibt stabil: ein weiterer valider Task danach wird verarbeitet.
    """
    # Temporäre Override-Datei mit aggressivem Timeout anlegen
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        suffix=".yml",
        prefix="taskgrid_timeout_override_",
        delete=False,
    ) as f:
        f.write(OVERRIDE_CONTENT)
        override_path = f.name

    try:
        # ── 1. System mit Override hochfahren ──────────────────────────────
        up = _docker_compose_with_override(
            override_path,
            "up", "-d", "--build", "--remove-orphans",
        )
        assert up.returncode == 0, (
            f"docker compose up fehlgeschlagen:\n{up.stderr or up.stdout}"
        )

        try:
            # ── 2. Warten bis alle nötigen Services laufen ─────────────────
            _wait_for_service_state(
                "namensdienst", expected_state="running",
                timeout=180, override_path=override_path,
            )
            _wait_for_service_state(
                "dispatcher", expected_state="running",
                timeout=180, override_path=override_path,
            )
            # worker-wait muss laufen und registriert sein,
            # damit der Task tatsächlich dispatcht wird.
            _wait_for_service_state(
                "worker-wait", expected_state="running",
                timeout=180, override_path=override_path,
            )
            # Kurze Pause damit worker-wait sich vollständig beim Namensdienst
            # registriert hat (Heartbeat-Intervall = 5 s).
            time.sleep(6)

            # ── 3. Task senden: wait 10 Sekunden ──────────────────────────
            # Der Dispatcher-Timeout feuert nach 3 s → Task wird FAILED
            # bevor der Worker nach 10 s fertig ist.
            task_id = _post_task("wait", str(WAIT_TASK_DURATION_SECS), "timeout-late-result-test")

            # ── 4. Polling: warten bis Task den terminalen Status FAILED hat ─
            # Muss innerhalb von ~(DISPATCH_TIMEOUT_SECS + Puffer) passieren.
            poll_timeout = TEST_DISPATCH_TIMEOUT_SECS + 10
            terminal_result = _poll_until_terminal(task_id, timeout=poll_timeout)

            assert terminal_result is not None, \
                "Dispatcher hat keine Antwort auf GetResult geliefert."
            assert terminal_result.payload.found, \
                f"Task {task_id} wurde im Dispatcher nicht gefunden."
            assert terminal_result.payload.status == "FAILED", (
                f"Erwartet: Status 'FAILED' nach Timeout, "
                f"tatsächlich: '{terminal_result.payload.status}'. "
                f"Timeout ({TEST_DISPATCH_TIMEOUT_SECS}s) hat möglicherweise nicht gefeuert."
            )

            # Status und result zum Zeitpunkt des Timeouts festhalten
            status_after_timeout = terminal_result.payload.status
            result_after_timeout = terminal_result.payload.result

            # ── 5. Warten bis Worker sein verspätetes Ergebnis gesendet hat ─
            # Der Worker braucht WAIT_TASK_DURATION_SECS (10s) ab Dispatch.
            # Wir warten die verbleibende Zeit plus Puffer.
            time.sleep(LATE_RESULT_GRACE_SECS)

            # ── 6. Sicherstellen: Status wurde NICHT überschrieben ─────────
            result_after_late_return = _get_result(task_id)

            assert result_after_late_return.payload.status == "FAILED", (
                f"Task-Status wurde durch verspätetes RESULT_RETURN überschrieben! "
                f"Vor verspätetem Ergebnis: '{status_after_timeout}', "
                f"danach: '{result_after_late_return.payload.status}'. "
                f"Der Dispatcher darf terminale Tasks nicht mehr verändern."
            )
            assert result_after_late_return.payload.result == result_after_timeout, (
                f"task.result wurde durch verspätetes Worker-Ergebnis überschrieben! "
                f"Vor verspätetem Ergebnis: '{result_after_timeout}', "
                f"danach: '{result_after_late_return.payload.result}'. "
                f"Terminale Tasks dürfen nicht mehr verändert werden."
            )

            # ── 7. Log-Prüfung: event=RESULT_RETURN_late muss vorhanden sein ─
            # Format (aus logger.py): "task_id=<X> event=RESULT_RETURN_late ..."
            dispatcher_logs = _get_dispatcher_logs()
            assert "RESULT_RETURN_late" in dispatcher_logs, (
                f"Erwarteter Log-Eintrag 'event=RESULT_RETURN_late' fehlt in den "
                f"Dispatcher-Logs. Das verspätete Ergebnis wurde entweder nicht "
                f"empfangen oder nicht korrekt protokolliert.\n"
                f"Dispatcher-Logs (Auszug, letzte 3000 Zeichen):\n"
                f"{dispatcher_logs[-3000:]}"
            )
            # task_id muss im selben Log-Kontext erscheinen
            assert f"task_id={task_id}" in dispatcher_logs, (
                f"task_id={task_id} fehlt im Dispatcher-Log. "
                f"RESULT_RETURN_late wurde möglicherweise für einen anderen Task geloggt."
            )

            # ── 8. Stabilitätsnachweis: valider Task nach dem Fehlerfall ───
            # Wenn der Dispatcher abgestürzt wäre, schlägt dieser Schritt fehl.
            stable_task_id = _post_task("wait", "1", "post-timeout-stability-check")
            stable_result  = _poll_until_terminal(
                stable_task_id, timeout=TERMINAL_POLL_TIMEOUT_SECS
            )

            assert stable_result is not None, \
                "Kein Ergebnis für den Stabilitäts-Task erhalten."
            assert stable_result.payload.status == "COMPLETED", (
                f"Dispatcher/Worker ist nach dem Timeout-Szenario instabil. "
                f"Stabilitäts-Task (wait=1s) Status: '{stable_result.payload.status}'. "
                f"Erwartung: 'COMPLETED'."
            )
            assert stable_result.payload.result == "waited 1s", (
                f"Falsches Ergebnis für Stabilitäts-Task: "
                f"erwartet 'waited 1s', erhalten '{stable_result.payload.result}'."
            )

        finally:
            _docker_compose_with_override(
                override_path,
                "down", "-v", "--remove-orphans",
            )

    finally:
        # Override-Datei immer aufräumen
        try:
            os.unlink(override_path)
        except OSError:
            pass