"""
Fehlertest: Ungültiger Payload (keine Zahlen) für den "sum"-Handler.

Erwartetes Verhalten (End-to-End):
  1. Client sendet send_task("sum", "abc,def,xyz")
  2. Worker versucht Payload zu parsen → ValueError → TaskProcessingError
  3. Worker sendet RESULT_RETURN mit status="FAILED" an Dispatcher
  4. Dispatcher speichert Fehlerresultat (status=FAILED, result=Fehlermeldung)
  5. Client bekommt bei GET_RESULT status=FAILED + verständliche Fehlermeldung
  6. Worker läuft weiter – kein Absturz (Nachweis: weiterer valider Task gelingt)
"""
import json
import os
import shutil
import subprocess
import time

import grpc
import pytest

from proto import taskgrid_pb2, taskgrid_pb2_grpc


REPO_ROOT          = os.path.dirname(os.path.dirname(__file__))
# Dispatcher lauscht extern auf Port 50051 (siehe docker-compose.yml)
DISPATCHER_ADDRESS = os.environ.get("DISPATCHER_ADDRESS", "localhost:50051")

# Maximale Wartezeit in Sekunden bis ein Task einen terminalen Status erreicht
RESULT_POLL_TIMEOUT = 45


# ── Docker-Compose-Hilfsfunktionen ────────────────────────────────────────────

def _docker_compose(*args: str) -> subprocess.CompletedProcess:
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
    return subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def _wait_for_service_state(
    service: str, expected_state: str = "running", timeout: int = 90
) -> None:
    """Wartet, bis der Container-Status eines Services den Erwartungswert enthält."""
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
                sender="pytest-error-test",
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
                sender="pytest-error-test",
                payload=taskgrid_pb2.GetResultRequest.Payload(task_id=task_id),
            )
        )


def _poll_until_terminal(task_id: int, timeout: int = RESULT_POLL_TIMEOUT) -> taskgrid_pb2.ResultResponse:
    """
    Fragt den Status eines Tasks im Polling-Verfahren ab, bis er einen
    terminalen Zustand (COMPLETED, FAILED, TIMEOUT) erreicht oder der
    Timeout abläuft.
    """
    terminal_states = {"COMPLETED", "FAILED", "TIMEOUT"}
    deadline = time.time() + timeout
    result = None
    while time.time() < deadline:
        result = _get_result(task_id)
        if result.payload.status in terminal_states:
            return result
        time.sleep(2)
    # Letzten bekannten Stand zurückgeben – Assertions im Test schlagen dann fehl
    return result


# ── Testfall ──────────────────────────────────────────────────────────────────

def test_worker_receives_invalid_payload():
    """
    Fehlerfall: Ungültiger Payload ("abc,def,xyz") für den "sum"-Task.

    Prüft das vollständige Fehler-Szenario:
      - Worker bricht mit FAILED ab (kein Absturz).
      - Dispatcher speichert FAILED + Fehlertext.
      - Client erhält verständliche Fehlermeldung.
      - Worker verarbeitet danach einen validen Task korrekt (kein Absturz).
    """
    up = _docker_compose("up", "-d", "--build", "--remove-orphans")
    assert up.returncode == 0, up.stderr or up.stdout

    try:
        # ── 1. Warten bis alle nötigen Services laufen ─────────────────────
        _wait_for_service_state("namensdienst", expected_state="running", timeout=180)
        _wait_for_service_state("dispatcher",   expected_state="running", timeout=180)
        # worker-sum muss ebenfalls bereit sein, damit der Task dispatcht wird
        _wait_for_service_state("worker-sum",   expected_state="running", timeout=180)

        # ── 2. Ungültigen Task senden ("abc,def,xyz" sind keine Zahlen) ────
        broken_task_id = _post_task("sum", "abc,def,xyz", "invalid-payload-test")

        # ── 3. Polling: warten bis Task terminalen Status hat ──────────────
        broken_result = _poll_until_terminal(broken_task_id, timeout=RESULT_POLL_TIMEOUT)

        # ── 4. Assertions: Task muss FAILED sein ──────────────────────────
        assert broken_result is not None, "Keine Antwort vom Dispatcher erhalten."
        assert broken_result.payload.found, \
            f"Task {broken_task_id} wurde im Dispatcher nicht gefunden."
        assert broken_result.payload.status == "FAILED", (
            f"Erwarteter Status 'FAILED', tatsächlich: "
            f"'{broken_result.payload.status}'. Antwort: {broken_result}"
        )

        # ── 5. Fehlermeldung muss verständlich und nicht leer sein ─────────
        # Der Dispatcher speichert den Fehlertext im result-Feld (da Task kein
        # separates error-Feld besitzt). Prüfen auf Inhalt.
        error_text = broken_result.payload.result or broken_result.payload.error
        assert error_text, (
            "Fehlermeldung ist leer – der Client würde keine Diagnose erhalten."
        )
        # Der Fehlertext sollte auf das Parsing-Problem hinweisen
        assert any(
            keyword in error_text.lower()
            for keyword in ("ungültig", "invalid", "zahlen", "fehler", "error", "abc")
        ), (
            f"Fehlermeldung enthält keinen verständlichen Hinweis auf die Ursache: "
            f"'{error_text}'"
        )

        # ── 6. Worker läuft weiter: validen Task danach erfolgreich senden ─
        # Würde der Worker abgestürzt sein, schlägt dieser Schritt fehl.
        valid_task_id = _post_task("sum", "3,7", "post-error-valid-task")
        valid_result = _poll_until_terminal(valid_task_id, timeout=RESULT_POLL_TIMEOUT)

        assert valid_result is not None, \
            "Kein Ergebnis für validen Task nach dem Fehlerfall erhalten."
        assert valid_result.payload.status == "COMPLETED", (
            f"Worker hat nach dem Fehlerfall keinen validen Task mehr verarbeitet "
            f"(Status: '{valid_result.payload.status}'). Worker möglicherweise abgestürzt."
        )
        assert valid_result.payload.result == "10", (
            f"Falsches Ergebnis für '3+7': erwartet '10', "
            f"erhalten '{valid_result.payload.result}'."
        )

    finally:
        _docker_compose("down", "-v", "--remove-orphans")