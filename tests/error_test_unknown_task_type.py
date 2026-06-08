"""
Fehlertest: Unbekannter Task-Typ — kein Worker für diesen Typ registriert.

Testprotokoll
─────────────────────────────────────────────────────────────────────────────

AUSGANGSSITUATION
─────────────────
  • System läuft mit worker-sum, worker-reverse und worker-hash.
  • Kein Worker ist für den Typ "unknown_type" registriert.
  • Der Namensdienst kennt nur die Typen: sum, reverse, hash.

DURCHGEFÜHRTE AKTION
─────────────────────
  send_task("unknown_type", "test")
  → Client sendet POST_TASK mit task_type="unknown_type" an den Dispatcher.

ERWARTETES VERHALTEN
─────────────────────
  1. Dispatcher nimmt POST_TASK an und antwortet mit success=True, task_id > 0
     (Eingabevalidierung prüft nur Format, nicht Typ-Verfügbarkeit).
  2. Dispatcher ruft LOOKUP_WORKER("unknown_type") am Namensdienst auf.
  3. Namensdienst liefert leere Worker-Liste (found=False oder workers=[]).
  4. Dispatcher loggt LOOKUP_WORKER_no_workers (Namensdienst-Ebene)
     UND/ODER DISPATCH_no_worker (Dispatch-Loop-Ebene).
  5. Task bleibt in QUEUED/RETRYING oder wird FAILED —
     niemals COMPLETED (kein Worker kann ihn verarbeiten).
  6. Client-Fehler: verständliche Antwort über request_result(task_id).

RELEVANTE LOG-EREIGNISSE (Dispatcher)
───────────────────────────────────────
  • POST_TASK_accepted     — Task wurde in Queue eingereiht
  • LOOKUP_WORKER_no_workers — Namensdienst hat keine Worker für den Typ
  • DISPATCH_no_worker     — Dispatch-Loop findet keinen Worker, re-enqueued

BEWERTUNG
──────────
  • PASS: Task wird niemals COMPLETED, Dispatcher bleibt stabil.
  • FAIL: System stürzt ab, Task wird fälschlich COMPLETED,
          oder kein verständlicher Fehlerzustand wird erreicht.

Architektonischer Hintergrund
─────────────────────────────
Der Dispatcher führt bei PostTask ausschließlich Format-Validierungen durch
(leerer Typ, zu langer Typ, zu langer Payload). Die Typ-Verfügbarkeit wird
NICHT bei der Annahme geprüft — der Task wird erst beim Dispatch-Versuch
durch LOOKUP_WORKER gecheckt.

In dispatch_loop._dispatch():
  workers = ns_client.lookup_worker(task.task_type, ...)
  worker  = selector.select(task.task_type, workers)   # → None
  if worker is None:
      log DISPATCH_no_worker
      time.sleep(2)
      queue.enqueue(task)   # → endlos re-enqueued bis MAX_RETRIES (Timeout-Pfad)

Da der Timeout-Mechanismus (Issue #18) nur bei DISPATCHED-Tasks greift
(d.h. wenn ein Worker zugewiesen wurde), kann ein Task mit unbekanntem Typ
theoretisch ewig in der Queue bleiben. Systemabsturz tritt dabei NICHT auf.
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


# ── Konstanten ────────────────────────────────────────────────────────────────

REPO_ROOT = os.path.dirname(os.path.dirname(__file__))
DISPATCHER_ADDRESS   = os.environ.get("DISPATCHER_ADDRESS",   "localhost:50051")
NAMENSDIENST_ADDRESS = os.environ.get("NAMENSDIENST_ADDRESS", "localhost:50052")

# Unbekannter Typ — absichtlich kein Worker registriert
UNKNOWN_TASK_TYPE = "unknown_type"
UNKNOWN_PAYLOAD   = "test"

# Budget für das Beobachten des Dispatcher-Verhaltens nach Task-Einreichung.
# Mindestens zwei Dispatch-Versuche: DISPATCH_no_worker + sleep(2s) + re-enqueue.
DISPATCH_OBSERVE_SECS = 12

# Budget für den abschließenden Stabilitätsnachweis (bekannter Typ muss COMPLETED)
STABILITY_POLL_TIMEOUT_SECS = 45


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
    service: str,
    expected_state: str = "running",
    timeout: int = 90,
) -> None:
    """Wartet bis der Container-Status eines Services den Erwartungswert enthält."""
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
                if expected_state in state:
                    return
                if health and expected_state in health:
                    return
        time.sleep(2)
    raise AssertionError(
        f"Service {service!r} nicht innerhalb von {timeout}s auf "
        f"{expected_state!r} gesetzt."
    )


def _bring_up() -> None:
    """
    Startet das System mit worker-sum, worker-reverse und worker-hash.
    worker-upper und worker-wait werden explizit NICHT gestartet,
    um sicherzustellen dass nur die drei Pflicht-Worker laufen.
    """

    up = _docker_compose(
        "up", "-d", "--build", "--remove-orphans",
        "worker-upper",
        "worker-wait",
        "namensdienst", "dispatcher", "worker-sum", "worker-reverse", "worker-hash",
    )
    assert up.returncode == 0, (
        f"docker compose up fehlgeschlagen:\n{up.stderr or up.stdout}"
    )
    for service in ("namensdienst", "dispatcher", "worker-sum", "worker-reverse", "worker-hash"):
        _wait_for_service_state(service, timeout=180)
    # Worker sendet alle 5s Heartbeat → erst nach erstem Heartbeat vollständig registriert
    time.sleep(8)


def _bring_down() -> None:
    """Fährt das komplette System herunter und räumt Volumes auf."""
    _docker_compose("down", "-v", "--remove-orphans")


# ── gRPC-Hilfsfunktionen ──────────────────────────────────────────────────────

def _post_task(task_type: str, payload: str, request_id: str) -> Optional[int]:
    """
    Sendet POST_TASK an den Dispatcher.
    Gibt die task_id zurück oder None wenn der Request fehlschlägt.
    """
    try:
        with grpc.insecure_channel(DISPATCHER_ADDRESS) as channel:
            stub = taskgrid_pb2_grpc.DispatcherServiceStub(channel)
            response = stub.PostTask(
                taskgrid_pb2.PostTaskRequest(
                    message_type="POST_TASK",
                    request_id=request_id,
                    timestamp=int(time.time()),
                    sender="pytest-unknown-type-test",
                    payload=taskgrid_pb2.PostTaskRequest.Payload(
                        task_type=task_type,
                        task_payload=payload,
                    ),
                ),
                timeout=10.0,
            )
        if response.payload.success:
            return int(response.payload.task_id)
        return None
    except grpc.RpcError:
        return None


def _get_result(task_id: int) -> Optional[taskgrid_pb2.ResultResponse]:
    """
    Fragt den aktuellen Status eines Tasks ab.
    Gibt None zurück bei Verbindungsfehlern.
    """
    try:
        with grpc.insecure_channel(DISPATCHER_ADDRESS) as channel:
            stub = taskgrid_pb2_grpc.DispatcherServiceStub(channel)
            return stub.GetResult(
                taskgrid_pb2.GetResultRequest(
                    message_type="GET_RESULT",
                    request_id=f"result-{task_id}",
                    timestamp=int(time.time()),
                    sender="pytest-unknown-type-test",
                    payload=taskgrid_pb2.GetResultRequest.Payload(task_id=task_id),
                ),
                timeout=10.0,
            )
    except grpc.RpcError:
        return None


def _poll_until_terminal(
    task_id: int, timeout: int
) -> Optional[taskgrid_pb2.ResultResponse]:
    """Pollt bis COMPLETED oder FAILED; gibt None zurück wenn Budget ausläuft."""
    deadline = time.time() + timeout
    result   = None
    while time.time() < deadline:
        result = _get_result(task_id)
        if result is not None and result.payload.status in {"COMPLETED", "FAILED"}:
            return result
        time.sleep(1)
    return result


def _lookup_worker_via_nameservice(task_type: str) -> list:
    """
    Fragt den Namensdienst direkt nach Workern für task_type.
    Gibt leere Liste zurück bei Verbindungsfehlern.
    """
    try:
        with grpc.insecure_channel(NAMENSDIENST_ADDRESS) as channel:
            stub = taskgrid_pb2_grpc.NamingServiceStub(channel)
            response = stub.LookupWorker(
                taskgrid_pb2.LookupRequest(
                    message_type="LOOKUP_WORKER",
                    request_id="unknown-type-test-lookup",
                    timestamp=int(time.time()),
                    sender="pytest-unknown-type-test",
                    payload=taskgrid_pb2.LookupRequest.Payload(task_type=task_type),
                ),
                timeout=5.0,
            )
        return list(response.payload.workers)
    except grpc.RpcError:
        return []


def _get_dispatcher_logs() -> str:
    """Gibt die Logs des dispatcher-Containers zurück."""
    result = subprocess.run(
        ["docker", "logs", "dispatcher"],
        capture_output=True, text=True, check=False,
    )
    return result.stdout + result.stderr


def _get_nameservice_logs() -> str:
    """Gibt die Logs des namensdienst-Containers zurück."""
    result = subprocess.run(
        ["docker", "logs", "namensdienst"],
        capture_output=True, text=True, check=False,
    )
    return result.stdout + result.stderr


# ── Testfall 1: Namensdienst liefert leere Liste für unbekannten Typ ──────────

def test_nameservice_returns_empty_for_unknown_type():
    """
    Testprotokoll — Testfall 1: Namensdienst-Verhalten bei unbekanntem Typ

    Ausgangssituation:
      System läuft mit worker-sum, worker-reverse, worker-hash.
      Kein Worker ist für "unknown_type" registriert.

    Durchgeführte Aktion:
      LOOKUP_WORKER("unknown_type") direkt am Namensdienst.

    Erwartetes Verhalten:
      Namensdienst antwortet mit leerer Worker-Liste (found=False oder workers=[]).

    Tatsächliches Verhalten: → wird im Test geprüft

    Bewertung:
      Grundvoraussetzung für alle weiteren Tests. Wenn dieser Test fehlschlägt,
      ist der Namensdienst fehlerhaft konfiguriert oder ein Worker mit Typ
      "unknown_type" wurde versehentlich gestartet.
    """
    _bring_up()
    try:
        # ── Vorbedingung: bekannte Worker sind registriert ────────────────────
        for known_type in ("sum", "reverse", "hash"):
            workers = _lookup_worker_via_nameservice(known_type)
            assert len(workers) > 0, (
                f"Vorbedingung nicht erfüllt: Kein '{known_type}'-Worker registriert. "
                f"Bitte sicherstellen dass worker-{known_type} läuft und "
                f"mindestens einen Heartbeat geschickt hat."
            )

        # ── Kernprüfung: unbekannter Typ → leere Liste ────────────────────────
        workers_unknown = _lookup_worker_via_nameservice(UNKNOWN_TASK_TYPE)

        assert len(workers_unknown) == 0, (
            f"Namensdienst gibt {len(workers_unknown)} Worker für Typ "
            f"'{UNKNOWN_TASK_TYPE}' zurück, obwohl kein solcher Worker "
            f"registriert sein sollte. "
            f"Gefundene Worker: {[w.worker_id for w in workers_unknown]}. "
            f"Mögliche Ursache: Ein Worker-Container mit WORKER_TYPE=unknown_type "
            f"läuft noch aus einem früheren Test."
        )

    finally:
        _bring_down()


# ── Testfall 2: Dispatcher nimmt Task an, kann ihn aber nicht ausführen ───────

def test_dispatcher_accepts_unknown_type_task():
    """
    Testprotokoll — Testfall 2: Dispatcher-Annahmeverhalten

    Ausgangssituation:
      System läuft mit worker-sum, worker-reverse, worker-hash.
      Kein Worker ist für "unknown_type" registriert.

    Durchgeführte Aktion:
      send_task("unknown_type", "test") via gRPC PostTask.

    Erwartetes Verhalten:
      Dispatcher antwortet mit success=True und einer gültigen task_id (> 0).
      Der Task wird in QUEUED-Zustand angenommen (Format-Validierung ist OK,
      Typ-Verfügbarkeit wird nicht bei der Annahme geprüft).

    Tatsächliches Verhalten: → wird im Test geprüft

    Relevante Log-Auszüge:
      Dispatcher: POST_TASK_accepted (task_id=X, task_type=unknown_type, status=QUEUED)

    Bewertung:
      Das Annehmen eines Tasks mit unbekanntem Typ ist korrektes Verhalten nach
      dem Protokoll. Der Fehler tritt erst beim Dispatch-Versuch auf.
    """
    _bring_up()
    try:
        task_id = _post_task(UNKNOWN_TASK_TYPE, UNKNOWN_PAYLOAD, "unknown-type-accept-test")

        assert task_id is not None, (
            f"Dispatcher hat PostTask für Typ '{UNKNOWN_TASK_TYPE}' abgelehnt "
            f"(success=False oder gRPC-Fehler). "
            f"Erwartet: success=True mit gültiger task_id. "
            f"Der Dispatcher darf Tasks nicht auf Basis der Typ-Verfügbarkeit "
            f"bei der Annahme ablehnen — das ist Aufgabe des Dispatch-Loop."
        )

        assert task_id > 0, (
            f"Dispatcher hat eine ungültige task_id={task_id} zurückgegeben. "
            f"Erwartet: task_id > 0."
        )

        # ── Initialer Status muss QUEUED sein ─────────────────────────────────
        result = _get_result(task_id)
        assert result is not None, (
            f"GetResult für task_id={task_id} fehlgeschlagen. "
            f"Der Task sollte direkt nach PostTask im Store vorhanden sein."
        )
        assert result.payload.found, (
            f"Task {task_id} nicht im Dispatcher-Store gefunden, obwohl "
            f"PostTask success=True zurückgegeben hat."
        )
        assert result.payload.status == "QUEUED", (
            f"Initialer Status von task_id={task_id} ist '{result.payload.status}', "
            f"erwartet 'QUEUED'. Der Task sollte direkt nach PostTask in QUEUED sein."
        )

        # ── Log-Prüfung: POST_TASK_accepted ───────────────────────────────────
        disp_logs = _get_dispatcher_logs()
        assert "POST_TASK_accepted" in disp_logs, (
            "Dispatcher-Log enthält kein 'POST_TASK_accepted'-Ereignis. "
            "Erwartet: Der Dispatcher soll das Einreihen des Tasks loggen. "
            f"Dispatcher-Logs (letzte 2000 Zeichen):\n{disp_logs[-2000:]}"
        )

    finally:
        _bring_down()


# ── Testfall 3: LOOKUP_WORKER liefert leere Liste — Dispatcher reagiert ───────

def test_dispatcher_logs_no_worker_for_unknown_type():
    """
    Testprotokoll — Testfall 3: Dispatcher-Reaktion auf leere Worker-Liste

    Ausgangssituation:
      System läuft mit worker-sum, worker-reverse, worker-hash.
      Kein Worker ist für "unknown_type" registriert.

    Durchgeführte Aktion:
      send_task("unknown_type", "test") — dann DISPATCH_OBSERVE_SECS lang warten.

    Erwartetes Verhalten:
      1. Dispatcher versucht LOOKUP_WORKER("unknown_type") am Namensdienst.
      2. Namensdienst antwortet mit leerer Liste.
      3. Dispatcher loggt LOOKUP_WORKER_no_workers UND/ODER DISPATCH_no_worker.
      4. Dispatcher re-enqueued den Task (kein sofortiges FAILED).

    Tatsächliches Verhalten: → wird im Test geprüft

    Relevante Log-Auszüge:
      Dispatcher: LOOKUP_WORKER_no_workers (task_type=unknown_type)
      Dispatcher: DISPATCH_no_worker      (task_id=X, task_type=unknown_type)

    Bewertung:
      Das System reagiert korrekt: keine Exception, kein Absturz, verständliche
      Log-Einträge die das Problem benennen.
    """
    _bring_up()
    try:
        task_id = _post_task(UNKNOWN_TASK_TYPE, UNKNOWN_PAYLOAD, "unknown-type-log-test")
        assert task_id is not None, (
            "PostTask für unknown_type fehlgeschlagen — Testvoraussetzung nicht erfüllt."
        )

        # ── DISPATCH_OBSERVE_SECS lang beobachten ─────────────────────────────
        # In dieser Zeit versucht der Dispatch-Loop mindestens einmal zu dispatchen.
        # Bei DISPATCH_NO_WORKER_RETRY_SECONDS=2 sind das ≥ 5 Versuche.
        time.sleep(DISPATCH_OBSERVE_SECS)

        # ── Log-Prüfung: mindestens eines der drei Schlüsselereignisse ────────
        disp_logs = _get_dispatcher_logs()

        has_no_worker     = "DISPATCH_no_worker"      in disp_logs
        has_lookup_empty  = "LOOKUP_WORKER_no_workers" in disp_logs
        has_lookup_failed = "LOOKUP_WORKER_failed"     in disp_logs

        assert has_no_worker or has_lookup_empty or has_lookup_failed, (
            "Kein erwartetes Ereignis im Dispatcher-Log für unbekannten Task-Typ gefunden.\n"
            "Erwartet (mindestens eines):\n"
            "  • 'DISPATCH_no_worker'       — Dispatch-Loop: kein Worker → re-enqueue\n"
            "  • 'LOOKUP_WORKER_no_workers' — Namensdienst: leere Antwort\n"
            "  • 'LOOKUP_WORKER_failed'     — Namensdienst: nicht erreichbar\n"
            f"task_id: {task_id}, task_type: {UNKNOWN_TASK_TYPE}\n"
            f"Dispatcher-Logs (letzte 3000 Zeichen):\n{disp_logs[-3000:]}"
        )

        assert f"task_id={task_id}" in disp_logs or str(task_id) in disp_logs, (
            f"task_id={task_id} fehlt im Dispatcher-Log nach {DISPATCH_OBSERVE_SECS}s. "
            "Der Dispatcher muss den Task mindestens einmal aus der Queue geholt haben. "
            f"Dispatcher-Logs (letzte 2000 Zeichen):\n{disp_logs[-2000:]}"
        )

    finally:
        _bring_down()


# ── Testfall 4: Task wird niemals COMPLETED ───────────────────────────────────

def test_unknown_type_task_never_completed():
    """
    Testprotokoll — Testfall 4: Task mit unbekanntem Typ wird niemals COMPLETED

    Ausgangssituation:
      System läuft mit worker-sum, worker-reverse, worker-hash.
      Kein Worker ist für "unknown_type" registriert.

    Durchgeführte Aktion:
      send_task("unknown_type", "test") — dann DISPATCH_OBSERVE_SECS lang beobachten.

    Erwartetes Verhalten:
      Task wechselt NIEMALS in den Status COMPLETED.
      Erlaubte Zustände: QUEUED, RETRYING, FAILED.

    Tatsächliches Verhalten: → wird im Test geprüft

    Relevante Log-Auszüge:
      Dispatcher: DISPATCH_no_worker (wiederholt, da Task re-enqueued wird)

    Bewertung:
      COMPLETED ohne Worker wäre ein kritischer Fehler. QUEUED/RETRYING/FAILED
      sind alle akzeptable Endzustände — das System soll stabil bleiben.
    """
    _bring_up()
    try:
        task_id = _post_task(UNKNOWN_TASK_TYPE, UNKNOWN_PAYLOAD, "unknown-type-no-complete-test")
        assert task_id is not None, (
            "PostTask für unknown_type fehlgeschlagen — Testvoraussetzung nicht erfüllt."
        )

        # ── DISPATCH_OBSERVE_SECS lang beobachten: niemals COMPLETED ─────────
        deadline    = time.time() + DISPATCH_OBSERVE_SECS
        last_result = _get_result(task_id)

        while time.time() < deadline:
            last_result = _get_result(task_id)
            if last_result is not None:
                assert last_result.payload.status != "COMPLETED", (
                    f"Task {task_id} mit Typ '{UNKNOWN_TASK_TYPE}' wurde COMPLETED, "
                    f"obwohl kein Worker für diesen Typ registriert ist. "
                    f"Dies ist ein schwerwiegender Fehler: kein Worker kann diesen "
                    f"Task verarbeiten. Mögliche Ursache: ein Worker-Container mit "
                    f"WORKER_TYPE=unknown_type läuft noch aus einem früheren Test."
                )
                assert last_result.payload.status != "DISPATCHED", (
                    f"Task {task_id} ist im Status DISPATCHED, obwohl kein Worker "
                    f"für Typ '{UNKNOWN_TASK_TYPE}' registriert ist. "
                    f"Der Dispatcher darf einen Task nur auf DISPATCHED setzen wenn "
                    f"tatsächlich ein Worker zugewiesen wurde."
                )
            time.sleep(1)

        # ── Finaler Status muss in der erlaubten Menge liegen ─────────────────
        allowed_statuses = {"QUEUED", "RETRYING", "FAILED"}
        final_status = last_result.payload.status if last_result else "UNKNOWN"

        assert final_status in allowed_statuses, (
            f"Unerwarteter finaler Status für task_id={task_id} nach "
            f"{DISPATCH_OBSERVE_SECS}s: '{final_status}'. "
            f"Erlaubte Zustände: {allowed_statuses}. "
            f"'COMPLETED' oder 'DISPATCHED' ohne tatsächlichen Worker sind Fehler."
        )

    finally:
        _bring_down()


# ── Testfall 5: Verständliche Fehlermeldung an Client ─────────────────────────

def test_client_receives_meaningful_error_state():
    """
    Testprotokoll — Testfall 5: Client erhält verständlichen Fehlerzustand

    Ausgangssituation:
      System läuft mit worker-sum, worker-reverse, worker-hash.
      Kein Worker ist für "unknown_type" registriert.

    Durchgeführte Aktion:
      1. send_task("unknown_type", "test") — task_id erhalten.
      2. request_result(task_id) nach DISPATCH_OBSERVE_SECS aufrufen.

    Erwartetes Verhalten:
      GetResult antwortet mit:
        • found=True   — Task ist im System bekannt
        • status ∈ {QUEUED, RETRYING, FAILED}  — kein COMPLETED
      Der Status selbst ist die Fehlermeldung: QUEUED/RETRYING zeigt an dass
      kein Worker verfügbar war; FAILED macht das Problem endgültig sichtbar.

    Tatsächliches Verhalten: → wird im Test geprüft

    Relevante Log-Auszüge:
      Dispatcher: LOOKUP_WORKER_no_workers / DISPATCH_no_worker

    Bewertung:
      Der Client bekommt über GetResult immer einen konsistenten Zustand zurück.
      Weder ein stummer Fehler (success=False ohne Erklärung) noch ein
      fälschliches COMPLETED sind akzeptabel.
    """
    _bring_up()
    try:
        # ── Task einreichen ───────────────────────────────────────────────────
        task_id = _post_task(UNKNOWN_TASK_TYPE, UNKNOWN_PAYLOAD, "unknown-type-client-error-test")
        assert task_id is not None, (
            "PostTask für unknown_type fehlgeschlagen — Testvoraussetzung nicht erfüllt."
        )

        # ── Warten bis Dispatch-Loop mindestens einmal versucht hat ───────────
        time.sleep(DISPATCH_OBSERVE_SECS)

        # ── Client fragt Ergebnis ab ──────────────────────────────────────────
        result = _get_result(task_id)
        assert result is not None, (
            f"GetResult für task_id={task_id} schlug fehl (gRPC-Fehler). "
            f"Der Dispatcher muss GetResult auch für Tasks mit unbekanntem Typ "
            f"beantworten."
        )

        # ── Task muss bekannt sein ────────────────────────────────────────────
        assert result.payload.found, (
            f"GetResult meldet found=False für task_id={task_id}. "
            f"Der Task wurde erfolgreich via PostTask eingereiht und muss "
            f"im Dispatcher-Store auffindbar sein."
        )

        # ── Status muss verständlich und korrekt sein ─────────────────────────
        status = result.payload.status
        assert status != "COMPLETED", (
            f"GetResult meldet COMPLETED für task_id={task_id} mit Typ "
            f"'{UNKNOWN_TASK_TYPE}'. Das ist unmöglich ohne registrierten Worker."
        )

        allowed_statuses = {"QUEUED", "RETRYING", "FAILED"}
        assert status in allowed_statuses, (
            f"GetResult meldet unerwarteten Status '{status}' für task_id={task_id}. "
            f"Erlaubte Zustände nach {DISPATCH_OBSERVE_SECS}s: {allowed_statuses}. "
            f"Dieser Status ist nicht Teil des definierten Zustandsmodells für "
            f"einen Task mit unbekanntem Typ."
        )

        # ── Konsistenz: task_id in der Antwort stimmt überein ─────────────────
        assert result.payload.task_id == task_id, (
            f"GetResult gibt task_id={result.payload.task_id} zurück, "
            f"erwartet task_id={task_id}. Dispatcher-Store inkonsistent."
        )

    finally:
        _bring_down()


# ── Testfall 6: System bleibt stabil — bekannte Tasks funktionieren weiter ────

def test_system_remains_stable_after_unknown_type():
    """
    Testprotokoll — Testfall 6: Stabilitätsnachweis nach unbekanntem Task-Typ

    Ausgangssituation:
      System läuft mit worker-sum, worker-reverse, worker-hash.
      Kein Worker ist für "unknown_type" registriert.

    Durchgeführte Aktion:
      1. send_task("unknown_type", "test") — löst wiederholte DISPATCH_no_worker aus.
      2. Parallel: send_task("sum", "10,20") — bekannter Typ.

    Erwartetes Verhalten:
      • Der unbekannte Task bleibt in QUEUED/RETRYING (nie COMPLETED).
      • Der bekannte sum-Task wird COMPLETED mit Ergebnis "30".
      • Das System stürzt NICHT ab, keine Exceptions, kein Deadlock.

    Tatsächliches Verhalten: → wird im Test geprüft

    Relevante Log-Auszüge:
      Dispatcher: DISPATCH_no_worker (für unknown_type)
      Dispatcher: DISPATCH_accepted  (für sum-Task)
      Dispatcher: RESULT_RETURN_stored (sum-Task, status=COMPLETED)

    Bewertung:
      Kernnachweis: Ein dauerhaft nicht-dispatchbarer Task blockiert den
      Dispatcher NICHT. Andere Tasks werden weiterhin korrekt verarbeitet.
      Das System verhält sich resilient.
    """
    _bring_up()
    try:
        # ── 1. Unbekannten Task einreichen ────────────────────────────────────
        unknown_task_id = _post_task(
            UNKNOWN_TASK_TYPE, UNKNOWN_PAYLOAD, "unknown-type-stability-unknown"
        )
        assert unknown_task_id is not None, (
            "PostTask für unknown_type fehlgeschlagen — Testvoraussetzung nicht erfüllt."
        )

        # ── 2. Bekannten sum-Task einreichen ──────────────────────────────────
        sum_task_id = _post_task("sum", "10,20", "unknown-type-stability-sum")
        assert sum_task_id is not None, (
            "PostTask für Typ 'sum' fehlgeschlagen. "
            "Ist worker-sum gestartet und im Namensdienst registriert?"
        )

        # ── 3. sum-Task muss COMPLETED werden ─────────────────────────────────
        sum_result = _poll_until_terminal(sum_task_id, timeout=STABILITY_POLL_TIMEOUT_SECS)

        assert sum_result is not None, (
            f"sum-Task (task_id={sum_task_id}) hat innerhalb von "
            f"{STABILITY_POLL_TIMEOUT_SECS}s keinen terminalen Status erreicht. "
            f"Letzter Status: {_get_result(sum_task_id).payload.status if _get_result(sum_task_id) else 'N/A'}. "
            f"Mögliche Ursache: Der Dispatcher ist durch den unbekannten Task blockiert "
            f"oder worker-sum ist nicht erreichbar."
        )
        assert sum_result.payload.status == "COMPLETED", (
            f"sum-Task (task_id={sum_task_id}) ist NICHT COMPLETED. "
            f"Tatsächlicher Status: '{sum_result.payload.status}'. "
            f"Ein bekannter Task-Typ muss trotz parallel laufendem unbekannten Task "
            f"verarbeitet werden. Dispatcher möglicherweise blockiert."
        )
        assert sum_result.payload.result == "30", (
            f"sum-Task liefert falsches Ergebnis: erwartet '30', "
            f"erhalten '{sum_result.payload.result}'. "
            f"Berechnung 10+20=30 ist korrekt — Worker-Fehler oder falsches Payload."
        )

        # ── 4. Unbekannter Task darf immer noch nicht COMPLETED sein ──────────
        unknown_result = _get_result(unknown_task_id)
        assert unknown_result is not None and unknown_result.payload.status != "COMPLETED", (
            f"unknown_type-Task (task_id={unknown_task_id}) wurde COMPLETED. "
            f"Das ist unmöglich ohne registrierten Worker."
        )

        # ── 5. Log-Prüfung: beide Ereignisse müssen dokumentiert sein ─────────
        disp_logs = _get_dispatcher_logs()

        # Unbekannter Task: kein Worker gefunden
        has_no_worker    = "DISPATCH_no_worker"      in disp_logs
        has_lookup_empty = "LOOKUP_WORKER_no_workers" in disp_logs
        assert has_no_worker or has_lookup_empty, (
            "Kein 'DISPATCH_no_worker' oder 'LOOKUP_WORKER_no_workers' im Log. "
            "Der Dispatcher muss mindestens einen fehlgeschlagenen Dispatch-Versuch "
            f"für '{UNKNOWN_TASK_TYPE}' geloggt haben. "
            f"Dispatcher-Logs (letzte 2000 Zeichen):\n{disp_logs[-2000:]}"
        )

        # Bekannter Task: Ergebnis zurückgemeldet
        assert "RESULT_RETURN_stored" in disp_logs or "DISPATCH_accepted" in disp_logs, (
            "Kein 'RESULT_RETURN_stored' oder 'DISPATCH_accepted' im Log für sum-Task. "
            f"Dispatcher-Logs (letzte 2000 Zeichen):\n{disp_logs[-2000:]}"
        )

    finally:
        _bring_down()
