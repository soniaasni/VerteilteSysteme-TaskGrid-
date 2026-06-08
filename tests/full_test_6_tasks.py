"""
Testprotokoll-Tests -- TaskGrid Distributed System
===================================================
Implementiert die in Testprotokoll.tex beschriebenen Tests T-01 bis T-06.

Voraussetzungen:
  - Docker und docker compose müssen installiert sein
  - Ausführung aus dem Projektverzeichnis (VerteilteSysteme-TaskGrid-/):
      pytest tests/test_protokoll.py -v -s

  - Oder mit laufendem System (Adressen via Umgebungsvariablen überschreibbar):
      DISPATCHER_ADDRESS=localhost:50051 pytest tests/test_protokoll.py -v

Getestete Tasktypen (gemäß Abschlusstabelle in Testprotokoll.tex):
  T-01  reverse   "hello world"       → "dlrow olleh"
  T-02  sum       "1,2,3"             → "6"
  T-03  hash      "hello"             → SHA256-Hexdigest
  T-04  upper     "Hello World 123"   → "HELLO WORLD 123"
  T-05  wait      "2"                 → "waited 2s"  (nach ≥ 2 s)
  T-06  sum       "10,20,30"          → "60"  (Zustandsverlauf-Verifikation)
"""

import hashlib
import json
import os
import shutil
import subprocess
import time

import grpc
import pytest

# ---------------------------------------------------------------------------
# Konfiguration
# ---------------------------------------------------------------------------

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DISPATCHER_ADDRESS = os.environ.get("DISPATCHER_ADDRESS", "localhost:50051")
NAMENSDIENST_ADDRESS = os.environ.get("NAMENSDIENST_ADDRESS", "localhost:50052")
DISPATCHER_HTTP_PORT = int(os.environ.get("DISPATCHER_HTTP_PORT", "8080"))

# Wartezeiten
STARTUP_TIMEOUT_SECS = 180
TASK_COMPLETION_TIMEOUT_SECS = 60
POLL_INTERVAL_SECS = 0.5


# ---------------------------------------------------------------------------
# Imports aus dem Projekt-Proto-Paket
# ---------------------------------------------------------------------------

# Das proto-Paket liegt im Repository-Root; pytest muss von dort gestartet werden.
try:
    from proto import taskgrid_pb2, taskgrid_pb2_grpc
except ImportError as exc:
    pytest.exit(
        f"Proto-Paket konnte nicht importiert werden: {exc}\n"
        "Bitte pytest aus dem Projektverzeichnis starten:\n"
        "  cd VerteilteSysteme-TaskGrid- && pytest tests/test_protokoll.py -v"
    )


# ---------------------------------------------------------------------------
# Docker-Hilfsfunktionen
# ---------------------------------------------------------------------------

def _docker_available() -> bool:
    """Prüft ob Docker und der Daemon verfügbar sind."""
    if shutil.which("docker") is None:
        return False
    result = subprocess.run(
        ["docker", "info"],
        capture_output=True, text=True, check=False,
    )
    return result.returncode == 0


def _docker_compose(*args: str) -> subprocess.CompletedProcess:
    """Führt docker compose im Projektverzeichnis aus."""
    if not _docker_available():
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
    """Wartet, bis ein Service gemäß 'docker compose ps' als 'running' gilt."""
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
                state = str(entry.get("State", "")).lower()
                health = str(entry.get("Health", "")).lower()
                if "running" in state or "healthy" in health:
                    return
        time.sleep(2)
    logs = _docker_compose("logs", "--no-color", service).stdout
    raise AssertionError(
        f"Service '{service}' nicht innerhalb von {timeout} s bereit.\nLogs:\n{logs}"
    )


def _docker_logs(service: str = "") -> str:
    """Gibt die Docker-Logs eines Services (oder aller) zurück."""
    args = ["logs", "--no-color"]
    if service:
        args.append(service)
    result = _docker_compose(*args)
    return result.stdout + result.stderr


# ---------------------------------------------------------------------------
# gRPC-Hilfsfunktionen
# ---------------------------------------------------------------------------

def _post_task(task_type: str, payload: str, request_id: str) -> int:
    """Sendet einen Task an den Dispatcher; gibt die task_id zurück."""
    with grpc.insecure_channel(DISPATCHER_ADDRESS) as channel:
        stub = taskgrid_pb2_grpc.DispatcherServiceStub(channel)
        resp = stub.PostTask(
            taskgrid_pb2.PostTaskRequest(
                message_type="POST_TASK",
                request_id=request_id,
                timestamp=int(time.time()),
                sender="protokoll-test",
                payload=taskgrid_pb2.PostTaskRequest.Payload(
                    task_type=task_type,
                    task_payload=payload,
                ),
            ),
            timeout=10.0,
        )
    assert resp.payload.success, (
        f"POST_TASK nicht erfolgreich: {resp.payload.message}"
    )
    assert resp.payload.task_id > 0, (
        f"Ungültige Task-ID erhalten: {resp.payload.task_id}"
    )
    assert resp.payload.status == "QUEUED", (
        f"Erwarteter Anfangsstatus QUEUED, erhalten: {resp.payload.status}"
    )
    return resp.payload.task_id


def _get_result(task_id: int) -> "taskgrid_pb2.ResultResponse":
    """Fragt den aktuellen Status/das Ergebnis eines Tasks ab."""
    with grpc.insecure_channel(DISPATCHER_ADDRESS) as channel:
        stub = taskgrid_pb2_grpc.DispatcherServiceStub(channel)
        return stub.GetResult(
            taskgrid_pb2.GetResultRequest(
                message_type="GET_RESULT",
                request_id=f"proto-gr-{task_id}",
                timestamp=int(time.time()),
                sender="protokoll-test",
                payload=taskgrid_pb2.GetResultRequest.Payload(task_id=task_id),
            ),
            timeout=10.0,
        )


class _PollResult:
    """
    Leichter Wrapper um eine ResultResponse, der zusätzlich die während des
    Pollings beobachteten Zustandsübergänge speichert.

    Alle Zugriffe auf `.payload` werden an die originale Protobuf-Response
    delegiert, sodass bestehende Aufrufer unverändert bleiben.
    """

    def __init__(
        self,
        response: "taskgrid_pb2.ResultResponse",
        observed_states: "list[str]",
    ) -> None:
        self._response = response
        self.observed_states = observed_states

    # Transparente Delegation aller anderen Attribute an die Protobuf-Response
    def __getattr__(self, name: str):
        return getattr(self._response, name)


def _poll_bis_completed(
    task_id: int,
    timeout: float = TASK_COMPLETION_TIMEOUT_SECS,
    track_states: bool = False,
) -> "_PollResult":
    """
    Polling-Loop: fragt GET_RESULT wiederholt ab, bis COMPLETED oder FAILED.
    Gibt ein _PollResult zurück, das die Protobuf-Response und (wenn
    track_states=True) die beobachtete Zustandssequenz enthält.
    """
    deadline = time.time() + timeout
    last_resp = None
    observed: list[str] = ["QUEUED"]
    last_status = "QUEUED"

    while time.time() < deadline:
        resp = _get_result(task_id)
        last_resp = resp
        current = resp.payload.status

        if track_states and current != last_status:
            observed.append(current)
            last_status = current

        if current in ("COMPLETED", "FAILED"):
            return _PollResult(resp, observed)
        time.sleep(POLL_INTERVAL_SECS)

    status = last_resp.payload.status if last_resp else "unbekannt"
    pytest.fail(
        f"Task {task_id} nicht abgeschlossen nach {timeout} s. "
        f"Letzter Status: {status}\n"
        f"Dispatcher-Logs:\n{_docker_logs('dispatcher')}"
    )


# ---------------------------------------------------------------------------
# Session-Fixture: Docker-Compose-Lebenszyklus
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session", autouse=True)
def docker_compose_up():
    """
    Startet das gesamte System einmalig pro Test-Session.
    Testumgebung: docker compose up -d --build  (wie in Testprotokoll.tex dokumentiert).
    Nach der Session werden Logs gesichert und die Container gestoppt.
    """
    if not _docker_available():
        pytest.skip("Docker oder Docker-Daemon nicht verfügbar.")

    # Alten Stack aufräumen
    _docker_compose("down", "--volumes", "--remove-orphans")
    time.sleep(2)

    # System starten
    result = _docker_compose("up", "-d", "--build")
    assert result.returncode == 0, (
        f"docker compose up --build fehlgeschlagen:\n{result.stderr or result.stdout}"
    )

    # Auf Namensdienst und Dispatcher warten
    _wait_for_service("namensdienst", timeout=STARTUP_TIMEOUT_SECS)
    _wait_for_service("dispatcher", timeout=STARTUP_TIMEOUT_SECS)

    # Workern Zeit zur Registrierung geben
    time.sleep(8)

    yield

    # Teardown: Logs sichern
    os.makedirs("smoke-test-logs", exist_ok=True)
    subprocess.run(
        ["docker", "cp", "dispatcher:/app/logs", "./smoke-test-logs"],
        check=False,
    )
    ns_logs = _docker_compose("logs", "--no-color", "namensdienst")
    with open(os.path.join("smoke-test-logs", "namensdienst.log"), "w", encoding="utf-8") as f:
        f.write(ns_logs.stdout + ns_logs.stderr)

    _docker_compose("down", "--volumes", "--remove-orphans")


# ===========================================================================
# T-01  reverse "hello world" → "dlrow olleh"
# ===========================================================================

class TestT01Reverse:
    """
    T-01 | Tasktyp: reverse | Eingabe: "hello world" | Erwartetes Ergebnis: "dlrow olleh"

    Prüft:
      - POST_TASK akzeptiert den Task (QUEUED)
      - Task erreicht Status COMPLETED
      - Ergebnis ist korrekt umgedreht ("dlrow olleh")
    """

    def test_t01_post_task_liefert_task_id_und_queued(self):
        """POST_TASK für 'reverse' gibt positive Task-ID und Status QUEUED zurück."""
        with grpc.insecure_channel(DISPATCHER_ADDRESS) as channel:
            stub = taskgrid_pb2_grpc.DispatcherServiceStub(channel)
            resp = stub.PostTask(
                taskgrid_pb2.PostTaskRequest(
                    message_type="POST_TASK",
                    request_id="t01-post",
                    timestamp=int(time.time()),
                    sender="protokoll-test",
                    payload=taskgrid_pb2.PostTaskRequest.Payload(
                        task_type="reverse",
                        task_payload="hello world",
                    ),
                ),
                timeout=10.0,
            )
        assert resp.payload.task_id > 0, (
            f"T-01: Ungültige Task-ID: {resp.payload.task_id}"
        )
        assert resp.payload.status == "QUEUED", (
            f"T-01: Erwarteter Anfangsstatus QUEUED, erhalten: {resp.payload.status}"
        )

    def test_t01_ergebnis_korrekt(self):
        """
        T-01 Haupttest: reverse("hello world") → COMPLETED mit Ergebnis "dlrow olleh".
        Statusverlauf: CREATED → QUEUED → DISPATCHED → PROCESSING → COMPLETED
        """
        task_id = _post_task("reverse", "hello world", "t01-main")

        resp = _poll_bis_completed(task_id)

        assert resp.payload.status == "COMPLETED", (
            f"T-01: Task endete mit '{resp.payload.status}', erwartet COMPLETED.\n"
            f"Logs:\n{_docker_logs('dispatcher')}"
        )
        assert resp.payload.result == "dlrow olleh", (
            f"T-01: Falsches Ergebnis.\n"
            f"  Erwartet: 'dlrow olleh'\n"
            f"  Erhalten: '{resp.payload.result}'"
        )

    def test_t01_ergebnis_ist_exakt_umgekehrt(self):
        """T-01 Zusatz: Das Ergebnis muss exakt der umgekehrte String sein."""
        payload = "hello world"
        task_id = _post_task("reverse", payload, "t01-exact")
        resp = _poll_bis_completed(task_id)
        assert resp.payload.result == payload[::-1], (
            f"T-01: Ergebnis stimmt nicht mit Umkehrung überein.\n"
            f"  Eingabe:  '{payload}'\n"
            f"  Erwartet: '{payload[::-1]}'\n"
            f"  Erhalten: '{resp.payload.result}'"
        )


# ===========================================================================
# T-02  sum "1,2,3" → "6"
# ===========================================================================

class TestT02Sum:
    """
    T-02 | Tasktyp: sum | Eingabe: "1,2,3" | Erwartetes Ergebnis: "6"

    Prüft:
      - POST_TASK akzeptiert den Task (QUEUED)
      - Task erreicht Status COMPLETED
      - Ergebnis ist die korrekte Summe ("6")
    """

    def test_t02_post_task_liefert_task_id_und_queued(self):
        """POST_TASK für 'sum' gibt positive Task-ID und Status QUEUED zurück."""
        with grpc.insecure_channel(DISPATCHER_ADDRESS) as channel:
            stub = taskgrid_pb2_grpc.DispatcherServiceStub(channel)
            resp = stub.PostTask(
                taskgrid_pb2.PostTaskRequest(
                    message_type="POST_TASK",
                    request_id="t02-post",
                    timestamp=int(time.time()),
                    sender="protokoll-test",
                    payload=taskgrid_pb2.PostTaskRequest.Payload(
                        task_type="sum",
                        task_payload="1,2,3",
                    ),
                ),
                timeout=10.0,
            )
        assert resp.payload.task_id > 0, (
            f"T-02: Ungültige Task-ID: {resp.payload.task_id}"
        )
        assert resp.payload.status == "QUEUED", (
            f"T-02: Erwarteter Anfangsstatus QUEUED, erhalten: {resp.payload.status}"
        )

    def test_t02_ergebnis_korrekt(self):
        """
        T-02 Haupttest: sum("1,2,3") → COMPLETED mit Ergebnis "6".
        Statusverlauf: CREATED → QUEUED → DISPATCHED → PROCESSING → COMPLETED
        """
        task_id = _post_task("sum", "1,2,3", "t02-main")

        resp = _poll_bis_completed(task_id)

        assert resp.payload.status == "COMPLETED", (
            f"T-02: Task endete mit '{resp.payload.status}', erwartet COMPLETED.\n"
            f"Logs:\n{_docker_logs('dispatcher')}"
        )
        assert resp.payload.result == "6", (
            f"T-02: Falsches Ergebnis.\n"
            f"  Erwartet: '6'\n"
            f"  Erhalten: '{resp.payload.result}'"
        )

    def test_t02_ergebnis_ist_integer_string(self):
        """T-02 Zusatz: Das Ergebnis ist ein gültiger Integer-String."""
        task_id = _post_task("sum", "1,2,3", "t02-int-check")
        resp = _poll_bis_completed(task_id)
        try:
            value = int(resp.payload.result)
        except ValueError:
            pytest.fail(
                f"T-02: Ergebnis ist kein Integer-String: '{resp.payload.result}'"
            )
        assert value == 6, (
            f"T-02: Summenwert inkorrekt. Erwartet 6, erhalten {value}."
        )


# ===========================================================================
# T-03  hash "hello" → SHA256-Hexdigest
# ===========================================================================

class TestT03Hash:
    """
    T-03 | Tasktyp: hash | Eingabe: "hello"
         | Erwartetes Ergebnis: 2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824

    Prüft:
      - POST_TASK akzeptiert den Task (QUEUED)
      - Task erreicht Status COMPLETED
      - Ergebnis ist der korrekte SHA256-Hexdigest
    """

    # Deterministisch extern verifizierbarer Wert (echo -n "hello" | sha256sum)
    EXPECTED_HASH = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"

    def test_t03_ergebnis_korrekt(self):
        """
        T-03 Haupttest: hash("hello") → COMPLETED mit korrektem SHA256-Hexdigest.
        Statusverlauf: CREATED → QUEUED → DISPATCHED → PROCESSING → COMPLETED
        """
        task_id = _post_task("hash", "hello", "t03-main")

        resp = _poll_bis_completed(task_id)

        assert resp.payload.status == "COMPLETED", (
            f"T-03: Task endete mit '{resp.payload.status}', erwartet COMPLETED.\n"
            f"Logs:\n{_docker_logs('dispatcher')}"
        )
        assert resp.payload.result == self.EXPECTED_HASH, (
            f"T-03: Falscher SHA256-Wert.\n"
            f"  Erwartet: '{self.EXPECTED_HASH}'\n"
            f"  Erhalten: '{resp.payload.result}'"
        )

    def test_t03_ergebnis_stimmt_mit_python_hashlib_ueberein(self):
        """T-03 Zusatz: Ergebnis muss mit hashlib.sha256('hello').hexdigest() übereinstimmen."""
        task_id = _post_task("hash", "hello", "t03-hashlib-check")
        resp = _poll_bis_completed(task_id)

        expected = hashlib.sha256("hello".encode("utf-8")).hexdigest()
        assert resp.payload.result == expected, (
            f"T-03: Hash weicht vom Python-hashlib-Ergebnis ab.\n"
            f"  hashlib: '{expected}'\n"
            f"  Worker:  '{resp.payload.result}'"
        )

    def test_t03_ergebnis_ist_gueltige_hex_zeichenkette(self):
        """T-03 Zusatz: SHA256-Ergebnis ist ein 64-Zeichen Hexstring."""
        task_id = _post_task("hash", "hello", "t03-hex-check")
        resp = _poll_bis_completed(task_id)

        result = resp.payload.result
        assert len(result) == 64, (
            f"T-03: Ergebnis hat {len(result)} Zeichen, erwartet 64."
        )
        assert all(c in "0123456789abcdef" for c in result), (
            f"T-03: Ergebnis enthält ungültige Hex-Zeichen: '{result}'"
        )


# ===========================================================================
# T-04  upper "Hello World 123" → "HELLO WORLD 123"
# ===========================================================================

class TestT04Upper:
    """
    T-04 | Tasktyp: upper | Eingabe: "Hello World 123" | Erwartetes Ergebnis: "HELLO WORLD 123"

    Prüft:
      - POST_TASK akzeptiert den Task (QUEUED)
      - Task erreicht Status COMPLETED
      - Ergebnis ist die korrekte Großschreibung
    """

    def test_t04_ergebnis_korrekt(self):
        """
        T-04 Haupttest: upper("Hello World 123") → COMPLETED mit Ergebnis "HELLO WORLD 123".
        Statusverlauf: CREATED → QUEUED → DISPATCHED → PROCESSING → COMPLETED
        """
        task_id = _post_task("upper", "Hello World 123", "t04-main")

        resp = _poll_bis_completed(task_id)

        assert resp.payload.status == "COMPLETED", (
            f"T-04: Task endete mit '{resp.payload.status}', erwartet COMPLETED.\n"
            f"Logs:\n{_docker_logs('dispatcher')}"
        )
        assert resp.payload.result == "HELLO WORLD 123", (
            f"T-04: Falsches Ergebnis.\n"
            f"  Erwartet: 'HELLO WORLD 123'\n"
            f"  Erhalten: '{resp.payload.result}'"
        )

    def test_t04_ergebnis_ist_vollstaendig_grossgeschrieben(self):
        """T-04 Zusatz: Das Ergebnis muss identisch mit payload.upper() sein."""
        payload = "Hello World 123"
        task_id = _post_task("upper", payload, "t04-upper-check")
        resp = _poll_bis_completed(task_id)

        assert resp.payload.result == payload.upper(), (
            f"T-04: Ergebnis stimmt nicht mit str.upper() überein.\n"
            f"  Eingabe:  '{payload}'\n"
            f"  Erwartet: '{payload.upper()}'\n"
            f"  Erhalten: '{resp.payload.result}'"
        )


# ===========================================================================
# T-05  wait "2" → "waited 2s"  (nach ≥ 2 s)
# ===========================================================================

class TestT05Wait:
    """
    T-05 | Tasktyp: wait | Eingabe: "2" (2 Sekunden)
         | Erwartetes Ergebnis: "waited 2s"  (Bearbeitungszeit ≥ 2 s)

    Prüft:
      - POST_TASK akzeptiert den Task (QUEUED)
      - Task erreicht Status COMPLETED nach mindestens 2 Sekunden
      - Ergebnis ist "waited 2s"
      - Die gemessene Wartezeit ist ≥ 2 s (Timing-Dokumentation)
    """

    def test_t05_ergebnis_korrekt(self):
        """
        T-05 Haupttest: wait("2") → COMPLETED mit Ergebnis "waited 2s".
        Statusverlauf: CREATED → QUEUED → DISPATCHED → PROCESSING → COMPLETED
        """
        task_id = _post_task("wait", "2", "t05-main")

        resp = _poll_bis_completed(task_id, timeout=30)

        assert resp.payload.status == "COMPLETED", (
            f"T-05: Task endete mit '{resp.payload.status}', erwartet COMPLETED.\n"
            f"Logs:\n{_docker_logs('dispatcher')}"
        )
        assert resp.payload.result == "waited 2s", (
            f"T-05: Falsches Ergebnis.\n"
            f"  Erwartet: 'waited 2s'\n"
            f"  Erhalten: '{resp.payload.result}'"
        )

    def test_t05_mindestwartezeit_wird_eingehalten(self):
        """
        T-05 Timing-Dokumentation: Ausführungszeit ≥ 2 s.
        Der wait-Handler ruft time.sleep(seconds) auf; Bearbeitungszeit muss ≥ 2 s betragen.
        """
        task_id = _post_task("wait", "2", "t05-timing")

        start = time.time()
        resp = _poll_bis_completed(task_id, timeout=30)
        elapsed = time.time() - start

        assert resp.payload.status == "COMPLETED", (
            f"T-05: Task endete mit '{resp.payload.status}', erwartet COMPLETED."
        )
        assert elapsed >= 2.0, (
            f"T-05: Gesamtdauer {elapsed:.2f} s < 2 s -- "
            f"wait-Handler hat Mindestwartezeit nicht eingehalten."
        )

    def test_t05_status_queued_sofort_nach_post(self):
        """T-05 Zusatz: POST_TASK antwortet sofort mit QUEUED (nicht wartend)."""
        start = time.time()
        task_id = _post_task("wait", "2", "t05-queued-check")
        elapsed = time.time() - start

        # POST_TASK soll sofort zurückkehren (< 5 s), auch wenn der Task 2 s dauert
        assert elapsed < 5.0, (
            f"T-05: POST_TASK dauerte {elapsed:.2f} s -- "
            f"Dispatcher sollte sofort mit QUEUED antworten."
        )
        # Ergebnis-Abfrage zum Abschluss (Clean-up des laufenden Tasks)
        _poll_bis_completed(task_id, timeout=30)


# ===========================================================================
# T-06  sum "10,20,30" → "60"  (Zustandsverlauf-Verifikation)
# ===========================================================================

class TestT06SumZustandsverlauf:
    """
    T-06 | Tasktyp: sum | Eingabe: "10,20,30" | Erwartetes Ergebnis: "60"
         | Zweck: Zustandsverlauf-Verifikation bei mehrfacher Nutzung desselben Workers.

    Prüft gemäß Testprotokoll-Abschnitt "Zweck dieses Tests":
      1. Dispatcher verwaltet mehrere aufeinanderfolgende Tasks desselben Typs korrekt.
      2. Statusübergang QUEUED → DISPATCHED → COMPLETED ist auch bei mehrfacher
         Nutzung desselben Workers konsistent.
      3. GET_RESULT-Mechanismus antwortet nach Abschluss korrekt.
    """

    def test_t06_ergebnis_korrekt(self):
        """
        T-06 Haupttest: sum("10,20,30") → COMPLETED mit Ergebnis "60".
        Statusverlauf: CREATED → QUEUED → DISPATCHED → PROCESSING → COMPLETED
        """
        task_id = _post_task("sum", "10,20,30", "t06-main")

        resp = _poll_bis_completed(task_id)

        assert resp.payload.status == "COMPLETED", (
            f"T-06: Task endete mit '{resp.payload.status}', erwartet COMPLETED.\n"
            f"Logs:\n{_docker_logs('dispatcher')}"
        )
        assert resp.payload.result == "60", (
            f"T-06: Falsches Ergebnis.\n"
            f"  Erwartet: '60'\n"
            f"  Erhalten: '{resp.payload.result}'"
        )

    def test_t06_zustandssequenz_vollstaendig(self):
        """
        T-06 Zustandsverlauf: Beobachtete Sequenz enthält QUEUED und COMPLETED.
        Interner Zustand CREATED ist vor QUEUED; extern sichtbar ab QUEUED (POST_TASK-Antwort).
        """
        task_id = _post_task("sum", "10,20,30", "t06-states")

        resp = _poll_bis_completed(task_id, track_states=True)
        observed = getattr(resp, "observed_states", ["QUEUED", resp.payload.status])

        assert "QUEUED" in observed, (
            f"T-06: QUEUED nicht in beobachteter Zustandssequenz: {observed}"
        )
        assert "COMPLETED" in observed, (
            f"T-06: COMPLETED nicht in beobachteter Zustandssequenz: {observed}"
        )
        assert resp.payload.status == "COMPLETED", (
            f"T-06: Endstatus ist nicht COMPLETED: {resp.payload.status}"
        )

    def test_t06_aufeinanderfolgende_sum_tasks_korrekt(self):
        """
        T-06 Punkt 1 + 2: Dispatcher verwaltet zwei aufeinanderfolgende sum-Tasks korrekt.
        Beide Tasks (aus Testprotokoll.tex: task_id=9 und task_id=10) werden konsistent abgeschlossen.
        """
        task_id_a = _post_task("sum", "10,20,30", "t06-seq-a")
        task_id_b = _post_task("sum", "5,10,15", "t06-seq-b")

        resp_a = _poll_bis_completed(task_id_a)
        resp_b = _poll_bis_completed(task_id_b)

        # Task A: 10+20+30 = 60
        assert resp_a.payload.status == "COMPLETED", (
            f"T-06: Task A endete mit '{resp_a.payload.status}', erwartet COMPLETED."
        )
        assert resp_a.payload.result == "60", (
            f"T-06: Task A falsches Ergebnis. Erwartet '60', erhalten '{resp_a.payload.result}'"
        )

        # Task B: 5+10+15 = 30
        assert resp_b.payload.status == "COMPLETED", (
            f"T-06: Task B endete mit '{resp_b.payload.status}', erwartet COMPLETED."
        )
        assert resp_b.payload.result == "30", (
            f"T-06: Task B falsches Ergebnis. Erwartet '30', erhalten '{resp_b.payload.result}'"
        )

    def test_t06_get_result_nach_abschluss_korrekt(self):
        """
        T-06 Punkt 3: GET_RESULT antwortet nach Task-Abschluss konsistent korrekt.
        Mehrfaches Abfragen nach COMPLETED liefert immer dasselbe Ergebnis.
        """
        task_id = _post_task("sum", "10,20,30", "t06-get-result")

        # Warten bis abgeschlossen
        first_resp = _poll_bis_completed(task_id)
        assert first_resp.payload.status == "COMPLETED"
        assert first_resp.payload.result == "60"

        # Mehrfaches erneutes Abfragen muss dasselbe Ergebnis liefern
        for i in range(3):
            repeat_resp = _get_result(task_id)
            assert repeat_resp.payload.status == "COMPLETED", (
                f"T-06: GET_RESULT Wiederholung {i + 1}: "
                f"Status '{repeat_resp.payload.status}' statt COMPLETED"
            )
            assert repeat_resp.payload.result == "60", (
                f"T-06: GET_RESULT Wiederholung {i + 1}: "
                f"Ergebnis '{repeat_resp.payload.result}' statt '60'"
            )


# ===========================================================================
# Gesamtzusammenfassung -- alle 6 Tests auf einmal
# ===========================================================================

class TestGesamtzusammenfassung:
    """
    Führt alle 6 in der Abschlusstabelle des Testprotokolls aufgeführten Tests durch.

    Tabelle (Testprotokoll.tex, Gesamtzusammenfassung):
      T-01  reverse  "hello world"       "dlrow olleh"
      T-02  sum      "1,2,3"             "6"
      T-03  hash     "hello"             SHA256-Wert
      T-04  upper    "Hello World 123"   "HELLO WORLD 123"
      T-05  wait     "2"                 "waited 2s"
      T-06  sum      "10,20,30"          "60"
    """

    TESTFAELLE = [
        # (test_nr, task_type, payload, expected_result)
        ("T-01", "reverse", "hello world", "dlrow olleh"),
        ("T-02", "sum", "1,2,3", "6"),
        ("T-03", "hash", "hello",
         "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"),
        ("T-04", "upper", "Hello World 123", "HELLO WORLD 123"),
        ("T-05", "wait", "2", "waited 2s"),
        ("T-06", "sum", "10,20,30", "60"),
    ]

    @pytest.mark.parametrize(
        "test_nr,task_type,payload,expected",
        TESTFAELLE,
        ids=[t[0] for t in TESTFAELLE],
    )
    def test_alle_protokoll_tests(
        self, test_nr: str, task_type: str, payload: str, expected: str
    ):
        """
        Parametrisierter Durchlauf aller 6 Protokoll-Testfälle.
        Jeder Task muss mit Status COMPLETED und dem erwarteten Ergebnis abschließen.
        """
        timeout = 30 if task_type == "wait" else TASK_COMPLETION_TIMEOUT_SECS
        task_id = _post_task(task_type, payload, f"summary-{test_nr.lower()}")
        resp = _poll_bis_completed(task_id, timeout=timeout)

        assert resp.payload.status == "COMPLETED", (
            f"{test_nr}: Task endete mit '{resp.payload.status}', erwartet COMPLETED.\n"
            f"Logs:\n{_docker_logs('dispatcher')}"
        )
        assert resp.payload.result == expected, (
            f"{test_nr} [{task_type}({payload!r})]:\n"
            f"  Erwartet: {expected!r}\n"
            f"  Erhalten: {resp.payload.result!r}"
        )