"""
gRPC-Servicer für den Dispatcher.
Implementiert: PostTask (#13), lookup_worker (#14), ReceiveResult (#16)
Stubs für:     GetResult (#17), GetStatus (#20)
"""

from __future__ import annotations

import grpc

from proto import taskgrid_pb2, taskgrid_pb2_grpc
from src.common.logger import get_logger, log_event
from src.common.protocol import TaskState, new_task_id
from src.dispatcher.namensdienst_client import NamensdienstClient
from src.dispatcher.state_machine import InvalidTransitionError, is_terminal, transition
from src.dispatcher.task import Task, MAX_TYPE_LEN, MAX_PAYLOAD_LEN
from src.dispatcher.task_queue import TaskQueue
from src.dispatcher.task_store import TaskStore
from src.dispatcher.worker_selector import RoundRobinSelector

logger = get_logger("dispatcher")


class DispatcherServicer(taskgrid_pb2_grpc.DispatcherServiceServicer):

    def __init__(self, store: TaskStore, task_queue: TaskQueue,
                 ns_client: NamensdienstClient,
                 dispatch_loop=None) -> None:
        self._store         = store
        self._queue         = task_queue
        self._ns_client     = ns_client
        self._selector      = RoundRobinSelector()
        self._dispatch_loop = dispatch_loop   # für cancel_timeout (#16)

    # ── Issue #14 ─────────────────────────────────────────────────────────────

    def lookup_worker(self, task_type: str, request_id: str = ""):
        """
        LOOKUP_WORKER: Fragt Namensdienst nach verfügbaren Workern für task_type
        und wählt einen per Round-Robin aus.
        Gibt WorkerInfo oder None zurück (kein Worker verfügbar / NS nicht erreichbar).
        Dispatcher-Code enthält KEINE statischen Worker-Adressen.
        """
        workers = self._ns_client.lookup_worker(task_type, request_id)
        selected = self._selector.select(task_type, workers)

        if selected is None:
            log_event(logger, "warning", "LOOKUP_WORKER_no_worker_selected",
                      request_id=request_id, task_type=task_type)
        else:
            log_event(logger, "info", "LOOKUP_WORKER_selected",
                      request_id=request_id,
                      task_type=task_type,
                      worker_id=selected.worker_id,
                      address=selected.address,
                      port=selected.port)
        return selected

    # ── Issue #13 ─────────────────────────────────────────────────────────────

    def PostTask(self, request, context):
        """
        POST_TASK: Client → Dispatcher
        Eingabe:  task_type, payload, sender, request_id
        Rückgabe: task_id + status=QUEUED  oder  gRPC-Fehlercode
        """
        rid = request.request_id

        # Eingabevalidierung (Aufgabenstellung: Fehlerfall "ungültige Payload")
        if not request.task_type:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("task_type darf nicht leer sein")
            log_event(logger, "warning", "POST_TASK_rejected",
                      request_id=rid, reason="missing_task_type")
            return taskgrid_pb2.TaskResponse()

        if len(request.task_type) > MAX_TYPE_LEN:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"task_type zu lang (max {MAX_TYPE_LEN} Zeichen)")
            log_event(logger, "warning", "POST_TASK_rejected",
                      request_id=rid, reason="task_type_too_long")
            return taskgrid_pb2.TaskResponse()

        if len(request.payload) > MAX_PAYLOAD_LEN:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"payload zu groß (max {MAX_PAYLOAD_LEN} Zeichen)")
            log_event(logger, "warning", "POST_TASK_rejected",
                      request_id=rid, reason="payload_too_large")
            return taskgrid_pb2.TaskResponse()

        # Task anlegen: CREATED → QUEUED
        task_id = new_task_id()
        task = Task(
            task_id=task_id,
            task_type=request.task_type,
            payload=request.payload,
            status=TaskState.CREATED,
        )
        task.status = TaskState.QUEUED

        # Speichern und einreihen
        self._store.add(task)
        self._queue.enqueue(task)

        log_event(logger, "info", "POST_TASK_accepted",
                  request_id=rid,
                  task_id=task_id,
                  task_type=request.task_type,
                  sender=request.sender,
                  queue_size=self._queue.size())

        return taskgrid_pb2.TaskResponse(
            task_id=task_id,
            status=TaskState.QUEUED.value,
        )

    # ── Stubs (werden in separaten Issues implementiert) ──────────────────────

    # ── Issue #17 ─────────────────────────────────────────────────────────────

    def GetResult(self, request, context):
        """
        GET_RESULT: Client → Dispatcher
        Eingabe:  task_id
        Rückgabe: ResultResponse(task_id, status, result)
          - COMPLETED:              result = Ergebnis
          - FAILED:                 result = Fehlermeldung
          - alle anderen Zustände:  result = "", status = aktueller Zustand
          - unbekannte task_id:     gRPC NOT_FOUND
        """
        rid     = request.request_id
        task_id = request.task_id

        task = self._store.get(task_id)
        if task is None:
            log_event(logger, "warning", "GET_RESULT_unknown_task",
                      request_id=rid, task_id=task_id, sender=request.sender)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Task {task_id} nicht gefunden")
            return taskgrid_pb2.ResultResponse(task_id=task_id, status="NOT_FOUND")

        log_event(logger, "info", "GET_RESULT_queried",
                  request_id=rid, task_id=task_id,
                  status=task.status.value, sender=request.sender)

        return taskgrid_pb2.ResultResponse(
            task_id=task_id,
            status=task.status.value,
            result=task.result,
        )

    def GetStatus(self, request, context):
        # Issue #20
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("GET_STATUS noch nicht implementiert (Issue #20)")
        return taskgrid_pb2.StatusResponse()

    # ── Issue #16 ─────────────────────────────────────────────────────────────

    def ReceiveResult(self, request, context):
        """
        RESULT_RETURN: Worker → Dispatcher
        Worker meldet fertiges Ergebnis (success=True) oder Fehler (success=False).
        Terminale Tasks (COMPLETED/FAILED) werden ignoriert (Idempotenz, §5).
        """
        rid     = request.request_id
        task_id = request.task_id

        task = self._store.get(task_id)
        if task is None:
            log_event(logger, "warning", "RESULT_RETURN_unknown_task",
                      request_id=rid, task_id=task_id, worker_id=request.worker_id)
            return taskgrid_pb2.Ack(ok=False, message="unknown task_id")

        # Verspätetes Ergebnis nach Timeout oder Duplikat → ignorieren
        if is_terminal(task):
            log_event(logger, "warning", "RESULT_RETURN_late",
                      request_id=rid, task_id=task_id,
                      worker_id=request.worker_id, status=task.status.value)
            return taskgrid_pb2.Ack(ok=True, message="already terminal")

        # Timeout-Timer abbrechen, da Ergebnis rechtzeitig eingetroffen
        if self._dispatch_loop is not None:
            self._dispatch_loop.cancel_timeout(task_id)

        new_state = TaskState.COMPLETED if request.success else TaskState.FAILED
        try:
            transition(task, new_state)
        except InvalidTransitionError as e:
            log_event(logger, "error", "RESULT_RETURN_invalid_transition",
                      request_id=rid, task_id=task_id, error=str(e))
            return taskgrid_pb2.Ack(ok=False, message=str(e))

        task.result = request.result if request.success else request.error_msg
        self._store.update(task)

        duration_ms = (
            (task.timestamp_completed - task.timestamp_dispatched) * 1000
            if task.timestamp_dispatched > 0 else 0
        )
        log_event(logger, "info", "RESULT_RETURN_stored",
                  request_id=rid,
                  task_id=task_id,
                  worker_id=request.worker_id,
                  status=task.status.value,
                  duration_ms=duration_ms)

        return taskgrid_pb2.Ack(ok=True)
