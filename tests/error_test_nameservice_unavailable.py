import json
import os
import shutil
import subprocess
import time

import grpc
import pytest

from proto import taskgrid_pb2, taskgrid_pb2_grpc


REPO_ROOT = os.path.dirname(os.path.dirname(__file__))
DISPATCHER_ADDRESS = os.environ.get("DISPATCHER_ADDRESS", "localhost:50051")


def _docker_compose(*args: str) -> subprocess.CompletedProcess:
    """Führt docker compose (oder docker-compose) aus und gibt das Ergebnis zurück."""
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


def _wait_for_service_state(service: str, expected_state: str = "running", timeout: int = 90) -> None:
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
                state = str(entry.get("State", "")).lower()
                health = str(entry.get("Health", "")).lower()
                if expected_state in state or (expected_state == "running" and "running" in state):
                    return
                if health and expected_state in health:
                    return
        time.sleep(2)

    raise AssertionError(f"Service {service!r} wurde nicht innerhalb von {timeout}s auf {expected_state!r} gesetzt.")


def _post_task(task_type: str, payload: str, request_id: str) -> int:
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


def _pause_nameservice() -> None:
    result = subprocess.run(["docker", "pause", "namensdienst"], cwd=REPO_ROOT, capture_output=True, text=True)
    if result.returncode != 0:
        result = subprocess.run(["docker", "stop", "namensdienst"], cwd=REPO_ROOT, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr or result.stdout


def _unpause_nameservice() -> None:
    result = subprocess.run(["docker", "unpause", "namensdienst"], cwd=REPO_ROOT, capture_output=True, text=True)
    if result.returncode != 0:
        result = subprocess.run(["docker", "start", "namensdienst"], cwd=REPO_ROOT, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr or result.stdout


def test_nameservice_unavailable_recovers_after_pause_and_unpause():
    """Fehlerfall: Namensdienst wird pausiert, Tasks können nicht mehr dispatcht werden und starten nach Recovery wieder."""
    # System komplett hochfahren.
    up = _docker_compose("up", "-d", "--build", "--remove-orphans")
    assert up.returncode == 0, up.stderr or up.stdout

    try:
        _wait_for_service_state("namensdienst", expected_state="running", timeout=180)
        _wait_for_service_state("dispatcher", expected_state="running", timeout=180)

        # Vor dem Fehlerfall: ein Task muss sauber verarbeitet werden.
        first_task_id = _post_task("sum", "1,2", "pre-pause")
        deadline = time.time() + 45
        first_result = None
        while time.time() < deadline:
            first_result = _get_result(first_task_id)
            if first_result.payload.status == "COMPLETED":
                break
            time.sleep(2)

        assert first_result is not None
        assert first_result.payload.status == "COMPLETED", first_result
        assert first_result.payload.result == "3", first_result

        # Namensdienst unterbrechen -> LOOKUP_WORKER kann nicht mehr funktionieren.
        _pause_nameservice()
        time.sleep(2)

        broken_task_id = _post_task("sum", "4,5", "during-pause")
        time.sleep(5)
        broken_result = _get_result(broken_task_id)

        # Der Task darf nach dem Namensdienst-Ausfall nicht erfolgreich abgeschlossen werden.
        assert broken_result.payload.task_id == broken_task_id
        assert broken_result.payload.status != "COMPLETED", broken_result

        # Nach 10 Sekunden wiederherstellen und erneut einen Task senden.
        time.sleep(10)
        _unpause_nameservice()
        _wait_for_service_state("namensdienst", expected_state="running", timeout=90)

        recovered_task_id = _post_task("sum", "7,8", "after-recovery")
        deadline = time.time() + 45
        recovered_result = None
        while time.time() < deadline:
            recovered_result = _get_result(recovered_task_id)
            if recovered_result.payload.status == "COMPLETED":
                break
            time.sleep(2)

        assert recovered_result is not None
        assert recovered_result.payload.status == "COMPLETED", recovered_result
        assert recovered_result.payload.result == "15", recovered_result
    finally:
        _docker_compose("down", "-v", "--remove-orphans")
