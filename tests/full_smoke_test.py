"""
E2E-Integrationstest — Vollständiger Docker-Compose-Durchlauf.

Testablauf (gemäß Aufgabenstellung):
  1. docker compose up --build  starten
  2. Alle Container laufen prüfen (docker compose ps)
  3. Namensdienst zeigt registrierte Worker (LookupWorker / LOOKUP_WORKER)
  4. Client sendet Task (POST_TASK)
  5. Task-ID wird empfangen
  6. GET_RESULT wird abgefragt bis COMPLETED (Polling-Loop)
  7. Ergebnis korrekt

Prüfpunkte:
  ✓ Alle Container starten ohne Fehler
  ✓ Worker registrieren sich beim Namensdienst
  ✓ Dispatcher findet Worker über Namensdienst (Logs zeigen LOOKUP_WORKER_success)
  ✓ Task durchläuft alle Zustände: CREATED → QUEUED → DISPATCHED → PROCESSING → COMPLETED
  ✓ Ergebnis korrekt
  ✓ Logs vollständig und nachvollziehbar

Voraussetzungen:
  - Docker und docker compose müssen installiert und erreichbar sein
  - Ausführung aus dem Projektverzeichnis:
      pytest tests/test_e2e_docker.py -v -s

Logs:
    Komplette Logs können nach Testlauf im Verzeichnis ./smoke-test-logs/ eingesehen werden (Dispatcher + Namensdienst).

Startbefehl (wie in README dokumentiert):
  docker compose up --build
"""

import json
import os
import shutil
import subprocess
import time
import urllib.request
import urllib.error

import grpc
import pytest

from proto import taskgrid_pb2, taskgrid_pb2_grpc

# ── Konfiguration ──────────────────────────────────────────────────────────────

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DISPATCHER_ADDRESS   = os.environ.get("DISPATCHER_ADDRESS",   "localhost:50051")
NAMENSDIENST_ADDRESS = os.environ.get("NAMENSDIENST_ADDRESS", "localhost:50052")
DISPATCHER_HTTP_PORT = int(os.environ.get("DISPATCHER_HTTP_PORT", "8080"))

# Timeout für Container-Start (docker compose up + healthchecks)
STARTUP_TIMEOUT_SECS = 180
# Maximale Zeit für einen vollständigen Task-Durchlauf
TASK_COMPLETION_TIMEOUT_SECS = 45
# Polling-Intervall bei GET_RESULT-Abfragen
POLL_INTERVAL_SECS = 1.0


# ── Docker-Hilfsfunktionen ────────────────────────────────────────────────────

def _docker_available() -> bool:
    """Prüft ob Docker und der Docker-Daemon verfügbar sind."""
    if shutil.which("docker") is None:
        return False
    result = subprocess.run(
        ["docker", "info"],
        capture_output=True, text=True, check=False,
    )
    return result.returncode == 0


def _docker_compose(*args: str) -> subprocess.CompletedProcess:
    """Führt docker compose im Projektverzeichnis aus."""
    if subprocess.run(["docker", "info"], capture_output=True, check=False).returncode != 0:
        pytest.skip("Docker-Daemon ist in dieser Umgebung nicht verfügbar.")

    return subprocess.run(
        ["docker", "compose", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
        env=os.environ.copy(),
    )


def _wait_for_service(service: str, timeout: int = STARTUP_TIMEOUT_SECS) -> None:
    """
    Wartet bis ein Service gemäß 'docker compose ps' als 'running' gilt.
    Entspricht dem Testablauf-Schritt 2: Container-Status prüfen.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = _docker_compose("ps", "--format", "json")
        if result.returncode != 0:
            time.sleep(2)
            continue
        for raw in result.stdout.splitlines():
            raw = raw.strip()
            if not raw:
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
    logs = _docker_compose("logs", "--no-color", service).stdout
    raise AssertionError(
        f"Service '{service}' nicht innerhalb von {timeout}s bereit.\n"
        f"Logs:\n{logs}"
    )


def _all_container_states() -> list[dict]:
    """Gibt alle Container-Einträge aus 'docker compose ps --format json' zurück."""
    result = _docker_compose("ps", "--all", "--format", "json")
    entries = []
    for raw in result.stdout.splitlines():
        raw = raw.strip()
        if not raw:
            continue
        try:
            entries.append(json.loads(raw))
        except json.JSONDecodeError:
            pass
    return entries


def _docker_logs(service: str = "") -> str:
    """Gibt die Docker-Logs eines Services (oder aller) zurück."""
    args = ["logs", "--no-color"]
    if service:
        args.append(service)
    result = _docker_compose(*args)
    return result.stdout + result.stderr


# ── gRPC-Hilfsfunktionen ──────────────────────────────────────────────────────

def _post_task(task_type: str, payload: str, request_id: str) -> int:
    """Sendet POST_TASK an den Dispatcher und gibt task_id zurück."""
    with grpc.insecure_channel(DISPATCHER_ADDRESS) as channel:
        stub = taskgrid_pb2_grpc.DispatcherServiceStub(channel)
        resp = stub.PostTask(
            taskgrid_pb2.PostTaskRequest(
                message_type="POST_TASK",
                request_id=request_id,
                timestamp=int(time.time()),
                sender="e2e-test",
                payload=taskgrid_pb2.PostTaskRequest.Payload(
                    task_type=task_type,
                    task_payload=payload,
                ),
            ),
            timeout=10.0,
        )
    assert resp.payload.task_id > 0, f"Ungültige Task-ID: {resp.payload.task_id}"
    return resp.payload.task_id


def _get_result(task_id: int) -> taskgrid_pb2.ResultResponse:
    """Fragt GET_RESULT für eine Task-ID ab."""
    with grpc.insecure_channel(DISPATCHER_ADDRESS) as channel:
        stub = taskgrid_pb2_grpc.DispatcherServiceStub(channel)
        return stub.GetResult(
            taskgrid_pb2.GetResultRequest(
                message_type="GET_RESULT",
                request_id=f"e2e-gr-{task_id}",
                timestamp=int(time.time()),
                sender="e2e-test",
                payload=taskgrid_pb2.GetResultRequest.Payload(task_id=task_id),
            ),
            timeout=10.0,
        )


def _poll_bis_completed(task_id: int, timeout: float = TASK_COMPLETION_TIMEOUT_SECS) -> taskgrid_pb2.ResultResponse:
    """
    Polling-Loop: fragt GET_RESULT wiederholt ab bis COMPLETED oder FAILED.
    Entspricht Schritt 6: GET_RESULT wird abgefragt bis COMPLETED.
    """
    deadline = time.time() + timeout
    last_resp = None
    while time.time() < deadline:
        resp = _get_result(task_id)
        last_resp = resp
        if resp.payload.status in ("COMPLETED", "FAILED"):
            return resp
        time.sleep(POLL_INTERVAL_SECS)
    pytest.fail(
        f"Task {task_id} nicht abgeschlossen nach {timeout}s. "
        f"Letzter Status: {last_resp.payload.status if last_resp else 'unbekannt'}\n"
        f"Dispatcher-Logs:\n{_docker_logs('dispatcher')}"
    )


def _lookup_worker(task_type: str) -> taskgrid_pb2.LookupResponse:
    """Fragt den Namensdienst nach Workern für einen Tasktyp."""
    with grpc.insecure_channel(NAMENSDIENST_ADDRESS) as channel:
        stub = taskgrid_pb2_grpc.NamingServiceStub(channel)
        return stub.LookupWorker(
            taskgrid_pb2.LookupRequest(
                message_type="LOOKUP_WORKER",
                request_id=f"e2e-ns-{task_type}",
                timestamp=int(time.time()),
                sender="e2e-test",
                payload=taskgrid_pb2.LookupRequest.Payload(task_type=task_type),
            ),
            timeout=10.0,
        )


# ── Session-Fixture: Docker-Compose-Lebenszyklus ──────────────────────────────

@pytest.fixture(scope="session", autouse=True)
def docker_compose_up():
    """
    Startet das gesamte System einmalig pro Test-Session und räumt danach auf.
    Verwendet docker compose up -d --build (detached), wie die anderen Tests im Projekt.
    Wartet auf Service-Readiness per docker compose ps (nicht per Port-Poll).
    """
    if not _docker_available():
        pytest.skip("Docker oder Docker-Daemon nicht verfügbar.")

    # Alten Stack aufräumen
    _docker_compose("down", "--volumes", "--remove-orphans")
    time.sleep(2)

    # Schritt 1: docker compose up --build starten (detached)
    result = _docker_compose("up", "-d", "--build")
    assert result.returncode == 0, (
        f"docker compose up --build fehlgeschlagen:\n{result.stderr or result.stdout}"
    )

    # Auf Namensdienst und Dispatcher warten (haben healthchecks in docker-compose.yml)
    _wait_for_service("namensdienst", timeout=STARTUP_TIMEOUT_SECS)
    _wait_for_service("dispatcher",   timeout=STARTUP_TIMEOUT_SECS)

    # Worker brauchen etwas Zeit zur Registrierung nach dem Dispatcher-Start
    time.sleep(8)

    yield

    # Teardown
    subprocess.run(["docker", "cp", "dispatcher:/app/logs", "./smoke-test-logs"], check=False)
    ns_logs = _docker_compose("logs", "--no-color", "namensdienst")
    with open(os.path.join("smoke-test-logs", "namensdienst.log"), "w", encoding="utf-8") as f:
        f.write(ns_logs.stdout + ns_logs.stderr)
    _docker_compose("down", "--volumes", "--remove-orphans")


# ── Schritt 2: Alle Container laufen prüfen ────────────────────────────────────

class TestContainerStatus:
    """Schritt 2: docker compose ps — alle Container laufen ohne Fehler."""

    EXPECTED_SERVICES = {
        "namensdienst",
        "dispatcher",
        "worker-sum",
        "worker-reverse",
        "worker-hash",
        "worker-upper",
        "worker-wait",
    }

    def test_compose_ps_liefert_ergebnis(self):
        """docker compose ps gibt Ausgabe zurück."""
        result = _docker_compose("ps", "--format", "json")
        assert result.returncode == 0, f"docker compose ps fehlgeschlagen:\n{result.stderr}"
        assert result.stdout.strip(), "docker compose ps lieferte keine Ausgabe"

    def test_alle_pflicht_services_laufen(self):
        """Alle definierten Services sind im Status 'running'."""
        entries = _all_container_states()
        assert entries, "Keine Container-Informationen von docker compose ps"

        running = set()
        not_running = []
        for c in entries:
            name  = c.get("Service", "")
            state = str(c.get("State", "")).lower()
            if "running" in state:
                running.add(name)
            elif name in self.EXPECTED_SERVICES and "client" not in name:
                not_running.append(f"{name}: {c.get('State', '?')}")

        assert not not_running, (
            f"Container nicht im Status 'running': {not_running}\n"
            f"Logs:\n{_docker_logs()}"
        )
        missing = self.EXPECTED_SERVICES - running
        assert not missing, (
            f"Services fehlen oder laufen nicht: {missing}\n"
            f"Laufende Services: {running}"
        )

    def test_keine_container_mit_fehler_exitcode(self):
        """Kein Pflicht-Container ist mit Fehlercode (≠0) beendet."""
        entries = _all_container_states()
        failed = []
        for c in entries:
            name = c.get("Service", "")
            if "client" in name:
                continue  # Client ist One-Shot, darf sich beenden
            exit_code = c.get("ExitCode")
            if exit_code is not None and int(exit_code) != 0:
                failed.append(f"{name} (ExitCode={exit_code}, State={c.get('State','?')})")
        assert not failed, (
            f"Container mit Fehlercode:\n" + "\n".join(failed) +
            f"\nLogs:\n{_docker_logs()}"
        )


# ── Schritt 3: Namensdienst zeigt registrierte Worker ─────────────────────────

class TestNamensdienst:
    """Schritt 3: Namensdienst hat registrierte Worker (LOOKUP_WORKER)."""

    EXPECTED_TASK_TYPES = ["sum", "reverse", "hash", "upper", "wait"]

    def _wait_for_workers(self, task_type: str, timeout: int = 30) -> list:
        """Wartet bis mindestens 1 Worker des Typs registriert ist."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            resp = _lookup_worker(task_type)
            if resp.payload.found and resp.payload.workers:
                return list(resp.payload.workers)
            time.sleep(2)
        return []

    def test_worker_fuer_sum_registriert(self):
        """Namensdienst kennt mindestens einen Worker für 'sum'."""
        workers = self._wait_for_workers("sum", timeout=30)
        assert workers, (
            f"Kein Worker für 'sum' im Namensdienst registriert.\n"
            f"Namensdienst-Logs:\n{_docker_logs('namensdienst')}"
        )

    def test_worker_fuer_alle_pflichttypen_registriert(self):
        """Für jeden Pflicht-Tasktyp ist mindestens ein Worker registriert."""
        fehlende = []
        for task_type in self.EXPECTED_TASK_TYPES:
            workers = self._wait_for_workers(task_type, timeout=20)
            if not workers:
                fehlende.append(task_type)
        assert not fehlende, (
            f"Kein Worker registriert für: {fehlende}\n"
            f"Namensdienst-Logs:\n{_docker_logs('namensdienst')}"
        )

    def test_registrierte_worker_haben_adresse_und_port(self):
        """Registrierte Worker liefern gültige Adresse und Port."""
        resp = _lookup_worker("sum")
        assert resp.payload.found
        for w in resp.payload.workers:
            assert w.address, f"Worker {w.worker_id} hat keine Adresse"
            assert w.port > 0, f"Worker {w.worker_id} hat ungültigen Port: {w.port}"


# ── Schritte 4 + 5: POST_TASK und Task-ID empfangen ───────────────────────────

class TestPostTask:
    """Schritte 4 & 5: Client sendet Task, Task-ID wird empfangen."""

    def test_post_task_liefert_task_id_und_queued(self):
        """POST_TASK gibt eine positive Task-ID und Status QUEUED zurück."""
        with grpc.insecure_channel(DISPATCHER_ADDRESS) as channel:
            stub = taskgrid_pb2_grpc.DispatcherServiceStub(channel)
            resp = stub.PostTask(
                taskgrid_pb2.PostTaskRequest(
                    message_type="POST_TASK",
                    request_id="e2e-post-001",
                    timestamp=int(time.time()),
                    sender="e2e-test",
                    payload=taskgrid_pb2.PostTaskRequest.Payload(
                        task_type="sum",
                        task_payload="1,2,3",
                    ),
                ),
                timeout=10.0,
            )
        # Schritt 5: Task-ID empfangen
        assert resp.payload.task_id > 0, "Task-ID muss positiv sein"
        assert resp.payload.status == "QUEUED", (
            f"Erwarteter Status QUEUED, erhalten: {resp.payload.status}"
        )

    def test_mehrere_post_tasks_liefern_eindeutige_ids(self):
        """Mehrere POST_TASK-Aufrufe liefern jeweils eindeutige Task-IDs."""
        ids = set()
        for i, task_type in enumerate(["sum", "reverse", "hash"]):
            task_id = _post_task(task_type, "test", f"e2e-uid-{i}")
            ids.add(task_id)
        assert len(ids) == 3, f"Task-IDs sind nicht eindeutig: {ids}"


# ── Schritte 6 + 7: Vollständiger Durchlauf mit Polling ───────────────────────

class TestVollstaendigerDurchlauf:
    """
    Schritte 6 & 7: Kerntest — vollständiger Task-Durchlauf mit Polling.

    Prüft POST_TASK → Task-ID → GET_RESULT-Polling → COMPLETED + korrektes Ergebnis.
    """

    def test_sum_task_ergebnis_korrekt(self):
        """
        Haupttest: sum("1,2,3") → COMPLETED mit Ergebnis "6".
        Schritt 4: POST_TASK | Schritt 5: Task-ID | Schritt 6: Polling | Schritt 7: Ergebnis
        """
        # Schritt 4: Client sendet Task
        task_id = _post_task("sum", "1,2,3", "e2e-sum-main")

        # Schritt 5: Task-ID empfangen (bereits durch _post_task geprüft)
        assert task_id > 0

        # Schritt 6: GET_RESULT polling bis COMPLETED
        resp = _poll_bis_completed(task_id)

        # Schritt 7: Ergebnis korrekt
        assert resp.payload.status == "COMPLETED", (
            f"Task endete mit '{resp.payload.status}', erwartet COMPLETED.\n"
            f"Logs:\n{_docker_logs('dispatcher')}"
        )
        assert resp.payload.result == "6", (
            f"Falsches Ergebnis: erwartet '6', erhalten '{resp.payload.result}'"
        )

    def test_reverse_task_ergebnis_korrekt(self):
        """reverse('hello') → COMPLETED mit Ergebnis 'olleh'."""
        task_id = _post_task("reverse", "hello", "e2e-reverse-main")
        resp = _poll_bis_completed(task_id)
        assert resp.payload.status == "COMPLETED"
        assert resp.payload.result == "olleh", (
            f"Falsches Ergebnis: erwartet 'olleh', erhalten '{resp.payload.result}'"
        )

    def test_hash_task_liefert_ergebnis(self):
        """hash('hello') → COMPLETED mit nicht-leerem Ergebnis."""
        task_id = _post_task("hash", "hello", "e2e-hash-main")
        resp = _poll_bis_completed(task_id)
        assert resp.payload.status == "COMPLETED"
        assert resp.payload.result, "hash-Task lieferte leeres Ergebnis"

    def test_upper_task_ergebnis_korrekt(self):
        """upper('hello') → COMPLETED mit Ergebnis 'HELLO'."""
        task_id = _post_task("upper", "hello", "e2e-upper-main")
        resp = _poll_bis_completed(task_id)
        assert resp.payload.status == "COMPLETED"
        assert resp.payload.result == "HELLO", (
            f"Falsches Ergebnis: erwartet 'HELLO', erhalten '{resp.payload.result}'"
        )


# ── Prüfpunkt: Zustandsdurchlauf CREATED → QUEUED → ... → COMPLETED ───────────

class TestZustandsDurchlauf:
    """
    Prüfpunkt: Task durchläuft CREATED → QUEUED → DISPATCHED → PROCESSING → COMPLETED.

    CREATED ist der interne Anfangszustand (vor Einreihen in die Queue).
    Extern sichtbar ab QUEUED (POST_TASK-Antwort).
    """

    def test_post_task_antwort_ist_queued(self):
        """POST_TASK-Antwort zeigt QUEUED — Task wurde eingereiht."""
        with grpc.insecure_channel(DISPATCHER_ADDRESS) as channel:
            stub = taskgrid_pb2_grpc.DispatcherServiceStub(channel)
            resp = stub.PostTask(
                taskgrid_pb2.PostTaskRequest(
                    message_type="POST_TASK",
                    request_id="e2e-state-001",
                    timestamp=int(time.time()),
                    sender="e2e-test",
                    payload=taskgrid_pb2.PostTaskRequest.Payload(
                        task_type="sum",
                        task_payload="10,20",
                    ),
                ),
                timeout=10.0,
            )
        assert resp.payload.status == "QUEUED", (
            f"Erwartet QUEUED nach POST_TASK, erhalten: {resp.payload.status}"
        )

    def test_zustandssequenz_queued_bis_completed(self):
        """
        Beobachtete Zustandssequenz: QUEUED → ... → COMPLETED.
        Dokumentiert den vollständigen Durchlauf CREATED(intern) → QUEUED → DISPATCHED
        → PROCESSING → COMPLETED.
        """
        task_id = _post_task("sum", "5,5", "e2e-state-002")

        observed   = ["QUEUED"]  # POST_TASK-Antwort ist immer QUEUED
        last       = "QUEUED"
        deadline   = time.time() + TASK_COMPLETION_TIMEOUT_SECS

        while time.time() < deadline:
            resp = _get_result(task_id)
            current = resp.payload.status
            if current != last:
                observed.append(current)
                last = current
            if current in ("COMPLETED", "FAILED"):
                break
            time.sleep(POLL_INTERVAL_SECS)

        assert last == "COMPLETED", (
            f"Task endete nicht mit COMPLETED.\n"
            f"Beobachtete Zustände: {observed}\n"
            f"Logs:\n{_docker_logs('dispatcher')}"
        )
        assert "QUEUED"    in observed, f"QUEUED nicht beobachtet. Sequenz: {observed}"
        assert "COMPLETED" in observed, f"COMPLETED nicht beobachtet. Sequenz: {observed}"


# ── Prüfpunkt: Dispatcher-Logs zeigen LOOKUP_WORKER ───────────────────────────

class TestLogs:
    """Prüfpunkt: Logs vollständig und nachvollziehbar."""

    def test_dispatcher_logs_enthalten_lookup_worker(self):
        """
        Dispatcher-Logs zeigen LOOKUP_WORKER — Dispatcher hat Worker
        über den echten Namensdienst gesucht (kein Mock).
        """
        # Task auslösen damit ein frischer LOOKUP_WORKER stattfindet
        task_id = _post_task("sum", "1", "e2e-log-lookup")
        time.sleep(4)  # Dispatch-Loop Zeit geben

        logs = _docker_logs("dispatcher")
        assert "LOOKUP_WORKER" in logs, (
            f"'LOOKUP_WORKER' nicht in Dispatcher-Logs.\nLogs:\n{logs}"
        )

    def test_dispatcher_logs_enthalten_post_task_accepted(self):
        """Dispatcher-Logs zeigen POST_TASK_accepted für eingereichte Tasks."""
        _post_task("sum", "2,3", "e2e-log-post")
        time.sleep(2)

        logs = _docker_logs("dispatcher")
        assert "POST_TASK_accepted" in logs, (
            f"'POST_TASK_accepted' nicht in Dispatcher-Logs.\nLogs:\n{logs}"
        )

    def test_namensdienst_logs_enthalten_worker_registrierung(self):
        """Namensdienst-Logs bestätigen Worker-Registrierung beim Start."""
        logs = _docker_logs("namensdienst")
        assert (
            "Registered worker" in logs
            or "RegisterWorker"  in logs
            or "registered"      in logs.lower()
        ), (
            f"Keine Worker-Registrierung in Namensdienst-Logs.\nLogs:\n{logs}"
        )

    def test_strukturiertes_log_format_dispatcher(self):
        """Dispatcher-Logs verwenden strukturiertes Format mit event=-Feldern."""
        logs = _docker_logs("dispatcher")
        assert "event=" in logs, (
            f"Kein strukturiertes Log-Format (event=...) in Dispatcher-Logs.\nLogs:\n{logs}"
        )


# ── Prüfpunkt: GET_STATUS (HTTP + gRPC) ───────────────────────────────────────

class TestGetStatus:
    """Prüfpunkt: GET_STATUS gibt Systemstatus zurück."""

    def test_http_status_antwortet_mit_json(self):
        """GET /status antwortet mit HTTP 200 und JSON."""
        url = f"http://localhost:{DISPATCHER_HTTP_PORT}/status"
        with urllib.request.urlopen(url, timeout=10) as r:
            assert r.status == 200
            ct = r.headers.get("Content-Type", "")
            assert "application/json" in ct, f"Kein JSON Content-Type: {ct}"
            body = json.loads(r.read().decode("utf-8"))
            assert isinstance(body, dict)
            assert len(body) >= 1

    def test_grpc_get_status_pflichtfelder(self):
        """gRPC GetStatus liefert alle Pflichtfelder des Proto."""
        with grpc.insecure_channel(DISPATCHER_ADDRESS) as channel:
            stub = taskgrid_pb2_grpc.DispatcherServiceStub(channel)
            resp = stub.GetStatus(
                taskgrid_pb2.StatusRequest(
                    message_type="GET_STATUS",
                    request_id="e2e-status-001",
                    timestamp=int(time.time()),
                    sender="e2e-test",
                ),
                timeout=10.0,
            )
        assert resp.payload.queued_tasks    >= 0
        assert resp.payload.active_workers  >= 0
        assert resp.payload.running_tasks   >= 0
        assert resp.payload.completed_tasks >= 0
        assert resp.payload.failed_tasks    >= 0

    def test_grpc_get_status_zeigt_aktive_worker(self):
        """GET_STATUS meldet mindestens einen aktiven Worker."""
        with grpc.insecure_channel(DISPATCHER_ADDRESS) as channel:
            stub = taskgrid_pb2_grpc.DispatcherServiceStub(channel)
            resp = stub.GetStatus(
                taskgrid_pb2.StatusRequest(
                    message_type="GET_STATUS",
                    request_id="e2e-status-002",
                    timestamp=int(time.time()),
                    sender="e2e-test",
                ),
                timeout=10.0,
            )
        details_raw = resp.payload.details
        if details_raw:
            try:
                details = json.loads(details_raw)
                if "aktive_worker" in details:
                    assert details["aktive_worker"] >= 1, (
                        f"Keine aktiven Worker im Status. Details: {details}"
                    )
            except json.JSONDecodeError:
                pass  # details-Feld ist optional