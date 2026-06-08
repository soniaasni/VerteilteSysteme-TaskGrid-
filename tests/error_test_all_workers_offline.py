"""
Fehlertest: Bekannter Tasktyp — aber alle Worker sind OFFLINE/UNHEALTHY.

Architektonischer Hintergrund
─────────────────────────────
Der Namensdienst verwaltet Worker-Registrierungen im RAM und prüft deren
Heartbeat-Status in einem Hintergrund-Loop (startLoop(5, 2)):

  • Kein Heartbeat >= 5s  → Status wechselt zu UNHEALTHY
  • Kein Heartbeat >= 10s → Status wechselt zu OFFLINE, Worker wird entfernt

LookupWorker filtert UNHEALTHY- und OFFLINE-Worker aus:
  result = [w for w in self.workers if w.status not in ("UNHEALTHY", "OFFLINE")]

Wenn worker-sum gestoppt wird, läuft folgender Übergang ab:

  t=0s   docker stop worker-sum → Container stoppt, Heartbeats enden
  t≈5s   Namensdienst-Loop: kein Heartbeat für 5s → ACTIVE → UNHEALTHY
           Log: "Worker <id> wurde UNHEALTHY gesetzt"
  t≈10s  Namensdienst-Loop: kein Heartbeat für 10s → UNHEALTHY → OFFLINE,
           Worker aus Liste entfernt
           Log: "Worker <id> wurde OFFLINE gesetzt und entfernt"

Nach t=5s liefert LookupWorker("sum") eine leere Liste zurück.
Der Dispatcher reagiert darauf mit DISPATCH_no_worker und re-enqueued den Task.
Sobald MAX_RETRIES (ohne Worker-Dispatch) erschöpft sind, bleibt der Task in
der Queue oder wird FAILED — abhängig von der Implementierung. Das Wichtigste:
Das System stürzt NICHT ab.

Parameter (hardcoded im System):
  HEARTBEAT_INTERVAL_SECONDS   = 5   (worker-sum, docker-compose.yml)
  NAMESERVICE_UNHEALTHY_SECS   = 5   (nameservice.py::startLoop(5, 2))
  NAMESERVICE_OFFLINE_SECS     = 10  (5s * 2, hardcoded Multiplikator)

Timeout für das Warten auf UNHEALTHY/OFFLINE:
  OFFLINE_POLL_TIMEOUT = NAMESERVICE_OFFLINE_SECS + 10s Puffer = 20s

Erwartetes Verhalten:
  1. Namensdienst markiert worker-sum ACTIVE → UNHEALTHY → OFFLINE
  2. LookupWorker("sum") liefert leere Liste zurück
  3. Dispatcher loggt DISPATCH_no_worker, hält Task in Queue oder markiert FAILED
  4. System bleibt stabil — kein Absturz, keine Exception

Zu dokumentierende Logs:
  • Namensdienst: "Worker <id> wurde UNHEALTHY gesetzt"
  • Namensdienst: "Worker <id> wurde OFFLINE gesetzt und entfernt"
  • Dispatcher:   LOOKUP_WORKER_no_workers (leere Antwort vom Namensdienst)
  • Dispatcher:   DISPATCH_no_worker       (kein Worker → Task re-enqueued)
"""

import json
import os
import shutil
import subprocess
import time
from typing import Optional

import grpc
import pytest

from proto import taskgrid_pb2, taskgrid_pb2_grpc


REPO_ROOT            = os.path.dirname(os.path.dirname(__file__))
DISPATCHER_ADDRESS   = os.environ.get("DISPATCHER_ADDRESS",  "localhost:50051")
NAMENSDIENST_ADDRESS = os.environ.get("NAMENSDIENST_ADDRESS", "localhost:50052")

# ── Timing-Konstanten ─────────────────────────────────────────────────────────
#
# Hardcodiert im System (nicht änderbar ohne Code-Änderung):
#   HEARTBEAT_INTERVAL_SECONDS = 5s  (docker-compose.yml, worker-sum)
#
NAMESERVICE_UNHEALTHY_SECS = os.environ.get("NAMESERVICE_UNHEALTHY_SECS",  10)
NAMESERVICE_OFFLINE_MULT = os.environ.get("NAMESERVICE_OFFLINE_MULT",  2)
NAMESERVICE_OFFLINE_SECS   = NAMESERVICE_UNHEALTHY_SECS * NAMESERVICE_OFFLINE_MULT

# Budget für das Warten bis OFFLINE + polling-Puffer
OFFLINE_POLL_TIMEOUT_SECS  = NAMESERVICE_OFFLINE_SECS + 10

# Budget für den Stabilitäts-Task (schnelle sum-Operation)
STABILITY_POLL_TIMEOUT_SECS = 45

# Wie lange beobachten wir den Dispatcher nach Task-Einreichung ohne Worker?
# Mindestens zwei Dispatch-Versuche (DISPATCH_no_worker + sleep 2s + re-enqueue)
DISPATCH_OBSERVE_SECS = 12


# ── Docker-Compose-Hilfsfunktionen ────────────────────────────────────────────

def _docker_compose(*args: str) -> subprocess.CompletedProcess:
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
        env=os.environ.copy(),
    )


def _wait_for_service_state(
    service: str,
    expected_state: str = "running",
    timeout: int = 90,
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
        f"Service {service!r} nicht innerhalb von {timeout}s auf {expected_state!r} gesetzt."
    )


def _stop_worker_sum() -> None:
    """
    Stoppt den worker-sum-Container graceful (docker stop).
    Kein SIGKILL — der Worker führt einen normalen Shutdown durch, schickt
    aber danach KEINE weiteren Heartbeats mehr.
    """
    result = subprocess.run(
        ["docker", "stop", "worker-sum"],
        capture_output=True, text=True, check=False,
    )
    # Ignoriere Fehler wenn Container unter anderem Namen läuft; Fallback über compose
    if result.returncode != 0:
        fallback = _docker_compose("stop", "worker-sum")
        assert fallback.returncode == 0, (
            f"docker stop worker-sum fehlgeschlagen:\n"
            f"{result.stderr}\n{fallback.stderr or fallback.stdout}"
        )


def _start_worker_sum() -> None:
    """Startet worker-sum neu (für cleanup / Recovery-Test)."""
    _docker_compose("start", "worker-sum")


# ── gRPC-Hilfsfunktionen ──────────────────────────────────────────────────────

def _post_task(task_type: str, payload: str, request_id: str) -> int:
    """Sendet POST_TASK an den Dispatcher und gibt die task_id zurück."""
    with grpc.insecure_channel(DISPATCHER_ADDRESS) as channel:
        stub = taskgrid_pb2_grpc.DispatcherServiceStub(channel)
        response = stub.PostTask(
            taskgrid_pb2.PostTaskRequest(
                message_type="POST_TASK",
                request_id=request_id,
                timestamp=int(time.time()),
                sender="pytest-all-workers-offline-test",
                payload=taskgrid_pb2.PostTaskRequest.Payload(
                    task_type=task_type,
                    task_payload=payload,
                ),
            )
        )
    assert response.payload.success, response.payload.message
    return int(response.payload.task_id)


def _get_result(task_id: int) -> taskgrid_pb2.ResultResponse:
    """Fragt den aktuellen Status/Ergebnis eines Tasks ab."""
    with grpc.insecure_channel(DISPATCHER_ADDRESS) as channel:
        stub = taskgrid_pb2_grpc.DispatcherServiceStub(channel)
        return stub.GetResult(
            taskgrid_pb2.GetResultRequest(
                message_type="GET_RESULT",
                request_id=f"result-{task_id}",
                timestamp=int(time.time()),
                sender="pytest-all-workers-offline-test",
                payload=taskgrid_pb2.GetResultRequest.Payload(task_id=task_id),
            )
        )


def _poll_until_terminal(
    task_id: int, timeout: int
) -> Optional[taskgrid_pb2.ResultResponse]:
    """Pollt bis COMPLETED oder FAILED; gibt None zurück wenn Budget ausläuft."""
    deadline = time.time() + timeout
    result   = None
    while time.time() < deadline:
        result = _get_result(task_id)
        if result.payload.status in {"COMPLETED", "FAILED"}:
            return result
        time.sleep(1)
    return result


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
                    request_id="offline-test-lookup",
                    timestamp=int(time.time()),
                    sender="pytest-all-workers-offline-test",
                    payload=taskgrid_pb2.LookupRequest.Payload(task_type=task_type),
                ),
                timeout=5.0,
            )
        return list(response.payload.workers)
    except grpc.RpcError:
        return []


def _get_nameservice_logs() -> str:
    """Gibt die Logs des namensdienst-Containers zurück."""
    result = subprocess.run(
        ["docker", "logs", "namensdienst"],
        capture_output=True, text=True, check=False,
    )
    return result.stdout + result.stderr


def _get_dispatcher_logs() -> str:
    """Gibt die Logs des dispatcher-Containers zurück."""
    result = subprocess.run(
        ["docker", "logs", "dispatcher"],
        capture_output=True, text=True, check=False,
    )
    return result.stdout + result.stderr


def _bring_up() -> None:
    """Fährt das komplette System hoch und wartet auf alle Services."""
    up = _docker_compose("up", "-d", "--build", "--remove-orphans")
    assert up.returncode == 0, (
        f"docker compose up fehlgeschlagen:\n{up.stderr or up.stdout}"
    )
    for service in ("namensdienst", "dispatcher", "worker-sum"):
        _wait_for_service_state(service, timeout=180)
    # Worker sendet alle 5s Heartbeat → erst nach erstem Heartbeat vollständig registriert
    time.sleep(6)


def _bring_down() -> None:
    """Fährt das komplette System herunter und räumt Volumes auf."""
    _docker_compose("down", "-v", "--remove-orphans")


# ── Testfall 1: Namensdienst markiert Worker UNHEALTHY dann OFFLINE ──────────

def test_nameservice_marks_worker_unhealthy_then_offline():
    """
    Prüft den vollständigen Statusübergang: ACTIVE → UNHEALTHY → OFFLINE.

    Ablauf:
      1. System hochfahren, sicherstellen dass worker-sum registriert ist.
      2. worker-sum stoppen (kein SIGKILL — kein Heartbeat mehr).
      3. Warten bis NAMESERVICE_UNHEALTHY_SECS + Puffer: LookupWorker muss
         leere Liste liefern (UNHEALTHY-Worker werden gefiltert).
      4. Warten bis NAMESERVICE_OFFLINE_SECS + Puffer: Worker muss vollständig
         entfernt sein.

    Zu dokumentierende Logs (namensdienst):
      • "Worker <id> wurde UNHEALTHY gesetzt"
      • "Worker <id> wurde OFFLINE gesetzt und entfernt"
    """
    _bring_up()
    try:
        # ── 1. Vorbedingung: worker-sum ist im Namensdienst registriert ───────
        workers_before = _lookup_worker_via_nameservice("sum")
        assert len(workers_before) > 0, (
            "Vorbedingung nicht erfüllt: Kein 'sum'-Worker im Namensdienst "
            "registriert. Ist der worker-sum-Container gestartet und hat "
            "mindestens einen Heartbeat geschickt?"
        )

        # ── 2. worker-sum stoppen ─────────────────────────────────────────────
        _stop_worker_sum()

        # ── 3. Warten bis UNHEALTHY → leere LookupWorker-Antwort ─────────────
        # Namensdienst setzt Worker auf UNHEALTHY nach NAMESERVICE_UNHEALTHY_SECS (5s).
        # LookupWorker filtert UNHEALTHY-Worker → leere Liste.
        unhealthy_deadline = time.time() + NAMESERVICE_UNHEALTHY_SECS + 8
        workers_mid = _lookup_worker_via_nameservice("sum")
        while time.time() < unhealthy_deadline and len(workers_mid) > 0:
            time.sleep(1)
            workers_mid = _lookup_worker_via_nameservice("sum")

        assert len(workers_mid) == 0, (
            f"Namensdienst gibt nach {NAMESERVICE_UNHEALTHY_SECS + 8}s noch "
            f"{len(workers_mid)} 'sum'-Worker zurück, obwohl worker-sum gestoppt "
            f"wurde. Erwartet: leere Liste (UNHEALTHY-Worker werden gefiltert). "
            f"Registrierte Worker: {[w.worker_id for w in workers_mid]}"
        )

        # ── 4. Warten bis OFFLINE → Worker vollständig entfernt ──────────────
        # Namensdienst entfernt Worker endgültig nach NAMESERVICE_OFFLINE_SECS (10s).
        offline_deadline = time.time() + OFFLINE_POLL_TIMEOUT_SECS
        workers_after = _lookup_worker_via_nameservice("sum")
        while time.time() < offline_deadline and len(workers_after) > 0:
            time.sleep(2)
            workers_after = _lookup_worker_via_nameservice("sum")

        assert len(workers_after) == 0, (
            f"Namensdienst gibt nach {OFFLINE_POLL_TIMEOUT_SECS}s noch "
            f"{len(workers_after)} 'sum'-Worker zurück. "
            f"Erwartet: vollständig entfernt (OFFLINE nach {NAMESERVICE_OFFLINE_SECS}s). "
            f"Registrierte Worker: {[w.worker_id for w in workers_after]}"
        )

        # ── 5. Log-Prüfung: Statusübergänge dokumentiert ─────────────────────
        ns_logs = _get_nameservice_logs()

        assert "UNHEALTHY" in ns_logs, (
            "Kein UNHEALTHY-Übergang in den Namensdienst-Logs gefunden. "
            "Erwartet: 'Worker <id> wurde UNHEALTHY gesetzt'. "
            f"Namensdienst-Logs (letzte 2000 Zeichen):\n{ns_logs[-2000:]}"
        )
        assert "OFFLINE" in ns_logs, (
            "Kein OFFLINE-Übergang in den Namensdienst-Logs gefunden. "
            "Erwartet: 'Worker <id> wurde OFFLINE gesetzt und entfernt'. "
            f"Namensdienst-Logs (letzte 2000 Zeichen):\n{ns_logs[-2000:]}"
        )

    finally:
        _bring_down()


# ── Testfall 2: LOOKUP_WORKER liefert leere Liste ────────────────────────────

def test_lookup_worker_returns_empty_list_when_all_offline():
    """
    Prüft direkt am Namensdienst, dass LookupWorker eine leere Liste zurückgibt
    sobald alle sum-Worker als UNHEALTHY oder OFFLINE markiert sind.

    Dies entspricht dem protokollierten Verhalten im Namensdienst:
      "Found 0 workers of type sum"

    Ablauf:
      1. System hochfahren, sum-Worker verifizieren.
      2. worker-sum stoppen.
      3. Warten bis Heartbeat-Timeout (UNHEALTHY_SECS + Puffer).
      4. LookupWorker direkt am Namensdienst aufrufen.
      5. Antwort muss `found=False` und leere `workers`-Liste enthalten.
    """
    _bring_up()
    try:
        # ── Vorbedingung: mind. ein sum-Worker aktiv ───────────────────────
        workers_initial = _lookup_worker_via_nameservice("sum")
        assert len(workers_initial) > 0, (
            "Vorbedingung nicht erfüllt: worker-sum ist nicht im Namensdienst "
            "registriert. Systemstart prüfen."
        )

        # ── worker-sum stoppen ─────────────────────────────────────────────
        _stop_worker_sum()

        # ── Warten bis Namensdienst Worker als UNHEALTHY markiert hat ─────
        time.sleep(NAMESERVICE_UNHEALTHY_SECS + 3)

        # ── Direkte LookupWorker-Abfrage am Namensdienst ──────────────────
        deadline = time.time() + OFFLINE_POLL_TIMEOUT_SECS
        workers = _lookup_worker_via_nameservice("sum")
        while time.time() < deadline and len(workers) > 0:
            time.sleep(1)
            workers = _lookup_worker_via_nameservice("sum")

        assert len(workers) == 0, (
            f"LookupWorker('sum') gibt {len(workers)} Worker zurück obwohl "
            f"worker-sum gestoppt und {NAMESERVICE_UNHEALTHY_SECS + 3}s vergangen sind. "
            f"UNHEALTHY/OFFLINE-Worker müssen aus der Antwort gefiltert werden. "
            f"Zurückgegebene Worker: {[(w.worker_id, w.status) for w in workers]}"
        )

        # ── Namensdienst-Logs: "Found 0 workers of type sum" ─────────────
        ns_logs = _get_nameservice_logs()
        assert "Found 0 workers of type sum" in ns_logs, (
            "Namensdienst hat keine 'Found 0 workers of type sum'-Meldung geloggt. "
            "Dies sollte bei jeder LookupWorker-Anfrage mit leerer Ergebnismenge "
            "erscheinen. "
            f"Namensdienst-Logs (letzte 1500 Zeichen):\n{ns_logs[-1500:]}"
        )

    finally:
        _bring_down()


# ── Testfall 3: Dispatcher hält Task in Queue oder markiert FAILED ────────────

def test_dispatcher_queues_or_fails_task_when_no_workers():
    """
    Prüft das Verhalten des Dispatchers wenn LookupWorker eine leere Liste
    liefert: Task bleibt in der Queue (QUEUED/RETRYING) oder wird FAILED.

    Konkret prüft dieser Test:
      • Task wird NICHT COMPLETED (kein Worker kann ihn verarbeiten).
      • Dispatcher loggt DISPATCH_no_worker (Worker-Suche schlug fehl).
      • Task-Status ist entweder QUEUED, RETRYING oder FAILED — nie COMPLETED.
      • Dispatcher stürzt nicht ab (kein Crash, keine unbehandelte Exception).

    Ablauf:
      1. System hochfahren, worker-sum registriert.
      2. worker-sum stoppen.
      3. Warten bis UNHEALTHY (leere LookupWorker-Antwort).
      4. sum-Task senden.
      5. DISPATCH_OBSERVE_SECS lang beobachten — nie COMPLETED.
      6. Dispatcher-Log auf DISPATCH_no_worker / LOOKUP_WORKER_no_workers prüfen.
    """
    _bring_up()
    try:
        # ── Vorbedingung: sum-Worker registriert ──────────────────────────
        workers_before = _lookup_worker_via_nameservice("sum")
        assert len(workers_before) > 0, (
            "Vorbedingung nicht erfüllt: worker-sum nicht registriert."
        )

        # ── worker-sum stoppen und warten bis UNHEALTHY ───────────────────
        _stop_worker_sum()

        # Pollen bis leere LookupWorker-Antwort → sicherstellen dass Task keinen
        # Worker mehr bekommt (UNHEALTHY + Puffer)
        deadline = time.time() + NAMESERVICE_UNHEALTHY_SECS + 8
        workers_mid = _lookup_worker_via_nameservice("sum")
        while time.time() < deadline and len(workers_mid) > 0:
            time.sleep(1)
            workers_mid = _lookup_worker_via_nameservice("sum")

        assert len(workers_mid) == 0, (
            "Namensdienst gibt noch sum-Worker zurück. "
            "Bitte Testvoraussetzung sicherstellen: worker-sum gestoppt und "
            f"Heartbeat-Timeout ({NAMESERVICE_UNHEALTHY_SECS}s) abgelaufen."
        )

        # ── sum-Task senden ────────────────────────────────────────────────
        task_id = _post_task("sum", "10,20", "offline-worker-dispatch-test")

        # ── DISPATCH_OBSERVE_SECS lang beobachten ─────────────────────────
        # Task darf in diesem Zeitraum NICHT COMPLETED werden
        deadline = time.time() + DISPATCH_OBSERVE_SECS
        last_result = _get_result(task_id)
        while time.time() < deadline:
            last_result = _get_result(task_id)
            assert last_result.payload.status != "COMPLETED", (
                f"Task {task_id} wurde COMPLETED obwohl alle sum-Worker offline sind. "
                f"Kein Worker kann den Task verarbeiten — COMPLETED ist hier unmöglich. "
                f"Dies deutet auf einen schwerwiegenden Fehler in der Worker-Prüfung hin."
            )
            time.sleep(1)

        # ── Erlaubte Endzustände: QUEUED, RETRYING oder FAILED ────────────
        allowed_statuses = {"QUEUED", "RETRYING", "FAILED"}
        assert last_result.payload.status in allowed_statuses, (
            f"Unerwarteter Task-Status nach {DISPATCH_OBSERVE_SECS}s ohne Worker: "
            f"'{last_result.payload.status}'. "
            f"Erlaubte Zustände: {allowed_statuses}. "
            f"COMPLETED wäre ein Fehler, DISPATCHED/PROCESSING ohne Worker ebenfalls."
        )

        # ── Dispatcher-Log: DISPATCH_no_worker oder LOOKUP_WORKER_no_workers ─
        disp_logs = _get_dispatcher_logs()

        has_no_worker      = "DISPATCH_no_worker"      in disp_logs
        has_lookup_empty   = "LOOKUP_WORKER_no_workers" in disp_logs
        has_lookup_failed  = "LOOKUP_WORKER_failed"     in disp_logs

        assert has_no_worker or has_lookup_empty or has_lookup_failed, (
            "Kein erwartetes Ereignis im Dispatcher-Log gefunden. "
            "Erwartet: 'DISPATCH_no_worker' ODER 'LOOKUP_WORKER_no_workers' "
            "ODER 'LOOKUP_WORKER_failed' (mindestens eines). "
            f"Dispatcher-Logs (letzte 3000 Zeichen):\n{disp_logs[-3000:]}"
        )

        assert f"task_id={task_id}" in disp_logs or str(task_id) in disp_logs, (
            f"task_id={task_id} fehlt im Dispatcher-Log. "
            "Der Dispatcher muss den Task mindestens einmal verarbeitet haben."
        )

    finally:
        _bring_down()


# ── Testfall 4: System bleibt stabil — kein Absturz ─────────────────────────

def test_system_remains_stable_after_all_workers_offline():
    """
    Stabilitätsnachweis: Das System stürzt nicht ab wenn alle sum-Worker offline sind.

    Ablauf:
      1. System hochfahren.
      2. worker-sum stoppen, bis OFFLINE.
      3. sum-Task senden — Dispatcher bekommt leere Worker-Liste.
      4. worker-sum wieder starten, bis ACTIVE im Namensdienst.
      5. Neuen sum-Task senden → muss COMPLETED werden.

    Damit wird bewiesen, dass der Dispatcher:
      - keine Exception wirft wenn keine Worker verfügbar sind
      - nach Worker-Recovery wieder normal dispatcht
      - keine hängenden Locks oder beschädigten Zustände hinterlässt
    """
    _bring_up()
    try:
        # ── 1. Vorbedingung: worker-sum aktiv ─────────────────────────────
        workers_before = _lookup_worker_via_nameservice("sum")
        assert len(workers_before) > 0, (
            "Vorbedingung nicht erfüllt: worker-sum nicht registriert."
        )

        # ── 2. worker-sum stoppen, bis Namensdienst ihn als OFFLINE markiert ─
        _stop_worker_sum()

        offline_deadline = time.time() + OFFLINE_POLL_TIMEOUT_SECS
        workers_mid = _lookup_worker_via_nameservice("sum")
        while time.time() < offline_deadline and len(workers_mid) > 0:
            time.sleep(2)
            workers_mid = _lookup_worker_via_nameservice("sum")

        assert len(workers_mid) == 0, (
            f"worker-sum ist nach {OFFLINE_POLL_TIMEOUT_SECS}s noch immer im "
            f"Namensdienst registriert. Heartbeat-Timeout nicht ausgelöst. "
            f"Registrierte Worker: {[w.worker_id for w in workers_mid]}"
        )

        # ── 3. sum-Task senden — kein Worker verfügbar ────────────────────
        offline_task_id = _post_task("sum", "5,6", "stability-offline-task")

        # Kurz beobachten — Task wird re-enqueued, nie COMPLETED
        time.sleep(DISPATCH_OBSERVE_SECS)
        offline_result = _get_result(offline_task_id)
        assert offline_result.payload.status != "COMPLETED", (
            f"Task {offline_task_id} wurde COMPLETED ohne verfügbare Worker. "
            "Dies ist nicht möglich und deutet auf einen schwerwiegenden Fehler hin."
        )

        # ── 4. worker-sum wieder starten, registrieren warten ─────────────
        _start_worker_sum()
        _wait_for_service_state("worker-sum", expected_state="running", timeout=90)
        # Warten bis Heartbeat gesendet und Worker registriert ist
        time.sleep(8)

        # Vorbedingung für Recovery: Worker muss wieder im Namensdienst sichtbar sein
        workers_recovered = _lookup_worker_via_nameservice("sum")
        assert len(workers_recovered) > 0, (
            "worker-sum wurde neu gestartet, ist aber nach 8s nicht im Namensdienst "
            "registriert. Heartbeat läuft möglicherweise nicht."
        )

        # ── 5. Recovery-Task: muss COMPLETED werden ───────────────────────
        recovery_task_id = _post_task("sum", "100,200", "stability-recovery-task")
        recovery_result  = _poll_until_terminal(recovery_task_id, timeout=STABILITY_POLL_TIMEOUT_SECS)

        assert recovery_result is not None, (
            f"Recovery-Task {recovery_task_id} hat innerhalb von "
            f"{STABILITY_POLL_TIMEOUT_SECS}s keinen terminalen Status erreicht. "
            f"Letzter Status: {_get_result(recovery_task_id).payload.status}. "
            "Dispatcher möglicherweise nach Worker-Ausfall instabil."
        )
        assert recovery_result.payload.status == "COMPLETED", (
            f"Recovery-Task (sum=100+200) sollte COMPLETED sein, "
            f"tatsächlich: '{recovery_result.payload.status}'. "
            "Dispatcher nach Worker-Recovery instabil."
        )
        assert recovery_result.payload.result == "300", (
            f"Falsches Ergebnis: erwartet '300', erhalten '{recovery_result.payload.result}'."
        )

    finally:
        _bring_down()


# ── Testfall 5: Kein COMPLETED wenn Worker UNHEALTHY (nicht nur OFFLINE) ─────

def test_no_completed_when_worker_unhealthy():
    """
    Negativtest: Ein Task darf auch im UNHEALTHY-Zustand des Workers (bevor OFFLINE)
    nicht COMPLETED werden, da LookupWorker UNHEALTHY-Worker bereits filtert.

    Prüft speziell das Zeitfenster zwischen UNHEALTHY (t=5s) und OFFLINE (t=10s):
    In diesem Fenster ist der Worker noch in der workers-Liste, aber mit
    status="UNHEALTHY". LookupWorker filtert ihn aus → keine Worker verfügbar.

    Ablauf:
      1. System hochfahren.
      2. worker-sum stoppen.
      3. Nach ~5s (UNHEALTHY-Threshold): Task senden.
      4. Task darf nicht COMPLETED werden.
    """
    _bring_up()
    try:
        # ── Vorbedingung: worker-sum aktiv ────────────────────────────────
        workers_before = _lookup_worker_via_nameservice("sum")
        assert len(workers_before) > 0, (
            "Vorbedingung nicht erfüllt: worker-sum nicht registriert."
        )

        # ── worker-sum stoppen ────────────────────────────────────────────
        _stop_worker_sum()

        # Warten bis UNHEALTHY eintritt (5s) + kurzen Puffer
        time.sleep(NAMESERVICE_UNHEALTHY_SECS + 2)

        # ── Task senden (Worker ist UNHEALTHY) ────────────────────────────
        task_id = _post_task("sum", "7,8", "unhealthy-worker-task")

        # ── Beobachten: Task darf nie COMPLETED werden ────────────────────
        deadline = time.time() + DISPATCH_OBSERVE_SECS
        last_result = _get_result(task_id)
        while time.time() < deadline:
            last_result = _get_result(task_id)
            assert last_result.payload.status != "COMPLETED", (
                f"Task {task_id} wurde COMPLETED obwohl worker-sum UNHEALTHY ist. "
                f"UNHEALTHY-Worker müssen von LookupWorker gefiltert werden. "
                f"Dies deutet darauf hin, dass der Namensdienst UNHEALTHY-Worker "
                f"fälschlicherweise noch in der Ergebnisliste zurückgibt."
            )
            time.sleep(1)

        # ── Finaler Status ist ein nicht-terminaler oder FAILED-Zustand ───
        final_status = last_result.payload.status
        assert final_status in {"QUEUED", "RETRYING", "FAILED"}, (
            f"Unerwarteter finaler Status: '{final_status}'. "
            f"Erwartet: QUEUED, RETRYING oder FAILED (nie COMPLETED oder DISPATCHED "
            f"ohne tatsächlichen Worker)."
        )

    finally:
        _bring_down()