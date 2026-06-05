"""
gRPC-Servicer für den Dispatcher.
Implementiert: PostTask (#13), lookup_worker (#14), ReturnResult (#16), GetResult (#17)
Stubs für:     GetStatus (#20)

Proto-Kompatibilität: Elena (worker/proto/taskgrid.proto)
  - ReturnResult statt ReceiveResult
  - task_id als int32 an gRPC-Grenze, intern als str
  - task_payload statt payload
  - status="COMPLETED"/"FAILED" statt success=bool
  - error statt error_msg
  - Ack.success statt Ack.ok
"""

from __future__ import annotations

import time
import grpc

from proto import taskgrid_pb2, taskgrid_pb2_grpc
from src.common.logger import get_logger, log_event
from src.common.protocol import TaskState, new_task_id
from src.dispatcher.namensdienst_client import NamensdienstClient
from src.dispatcher.state_machine import InvalidTransitionError, is_terminal, transition
from src.dispatcher.status_collector import StatusCollector
from src.dispatcher.task import Task, MAX_TYPE_LEN, MAX_PAYLOAD_LEN
from src.dispatcher.task_queue import TaskQueue
from src.dispatcher.task_store import TaskStore
from src.dispatcher.worker_selector import RoundRobinSelector

logger = get_logger("dispatcher")


class DispatcherServicer(taskgrid_pb2_grpc.DispatcherServiceServicer):

    def __init__(self, store: TaskStore, task_queue: TaskQueue,
                 ns_client: NamensdienstClient,
                 dispatch_loop=None,
                 collector: StatusCollector | None = None) -> None:
        self._store         = store
        self._queue         = task_queue
        self._ns_client     = ns_client
        self._selector      = RoundRobinSelector()
        self._dispatch_loop = dispatch_loop
        self._collector     = collector

    # ── Issue #14 ─────────────────────────────────────────────────────────────

    def lookup_worker(self, task_type: str, request_id: str = ""):
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
        Eingabe:  PostTaskRequest(request_id, task_type, task_payload, sender)
        Rückgabe: TaskResponse(task_id, status) oder gRPC-Fehlercode
        """
        rid       = request.request_id
        task_type = request.payload.task_type
        payload   = request.payload.task_payload

        if not task_type:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("task_type darf nicht leer sein")
            log_event(logger, "warning", "POST_TASK_rejected",
                      request_id=rid, reason="missing_task_type")
            return taskgrid_pb2.PostTaskResponse()

        if len(task_type) > MAX_TYPE_LEN:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"task_type zu lang (max {MAX_TYPE_LEN} Zeichen)")
            log_event(logger, "warning", "POST_TASK_rejected",
                      request_id=rid, reason="task_type_too_long")
            return taskgrid_pb2.PostTaskResponse()

        if len(payload) > MAX_PAYLOAD_LEN:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"payload zu groß (max {MAX_PAYLOAD_LEN} Zeichen)")
            log_event(logger, "warning", "POST_TASK_rejected",
                      request_id=rid, reason="payload_too_large")
            return taskgrid_pb2.PostTaskResponse()

        task_id = new_task_id()
        task = Task(
            task_id=task_id,
            task_type=task_type,
            payload=payload,
            status=TaskState.QUEUED,
        )

        self._store.add(task)
        self._queue.enqueue(task)

        log_event(logger, "info", "POST_TASK_accepted",
                  request_id=rid,
                  task_id=task_id,
                  task_type=task_type,
                  sender=request.sender,
                  status=TaskState.QUEUED.value,
                  queue_size=self._queue.size())

        return taskgrid_pb2.PostTaskResponse(
            message_type="POST_TASK_RESPONSE",
            request_id=rid,
            timestamp=int(time.time()),
            sender="dispatcher",
            payload=taskgrid_pb2.PostTaskResponse.Payload(
                success=True,
                task_id=int(task_id),
                status=TaskState.QUEUED.value,
            ),
        )

    # ── Issue #17 ─────────────────────────────────────────────────────────────

    def GetResult(self, request, context):
        """
        GET_RESULT: Client → Dispatcher
        Eingabe:  GetResultRequest(request_id, task_id: int32, sender)
        Rückgabe: ResultResponse(task_id, status, result)
        """
        rid     = request.request_id
        task_id = str(request.payload.task_id)   # int32 → string intern

        task = self._store.get(task_id)
        if task is None:
            log_event(logger, "warning", "GET_RESULT_unknown_task",
                      request_id=rid, task_id=task_id, sender=request.sender)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Task {task_id} nicht gefunden")
            return taskgrid_pb2.ResultResponse(
                message_type="RESULT_RESPONSE",
                request_id=rid,
                timestamp=int(time.time()),
                sender="dispatcher",
                payload=taskgrid_pb2.ResultResponse.Payload(
                    found=False,
                    task_id=request.payload.task_id,
                    status="NOT_FOUND",
                ),
            )

        log_event(logger, "info", "GET_RESULT_queried",
                  request_id=rid, task_id=task_id,
                  status=task.status.value, sender=request.sender)

        return taskgrid_pb2.ResultResponse(
            message_type="RESULT_RESPONSE",
            request_id=rid,
            timestamp=int(time.time()),
            sender="dispatcher",
            payload=taskgrid_pb2.ResultResponse.Payload(
                found=True,
                task_id=int(task_id),
                status=task.status.value,
                result=task.result or "",
            ),
        )

    # ── Issue #20 ─────────────────────────────────────────────────────────────

    def GetStatus(self, request, context):
        """
        GET_STATUS: Client → Dispatcher
        Gibt Monitoring-Überblick zurück. Vollständige Daten über HTTP /status.
        """
        if self._collector is None:
            return taskgrid_pb2.StatusResponse(
                payload=taskgrid_pb2.StatusResponse.Payload(details="{}"),
            )

        import json
        data = self._collector.get_status()
        log_event(logger, "info", "GET_STATUS_queried", sender=request.sender)
        return taskgrid_pb2.StatusResponse(
            message_type="STATUS_RESPONSE",
            request_id=request.request_id,
            timestamp=int(time.time()),
            sender="dispatcher",
            payload=taskgrid_pb2.StatusResponse.Payload(
                queued_tasks=data["offene_tasks"],
                running_tasks=data["laufende_tasks"],
                active_workers=data["aktive_worker"],
                completed_tasks=data["abgeschlossene_tasks"],
                failed_tasks=data["fehlgeschlagene_tasks"],
                timeout_count=data["anzahl_timeouts"],
                retry_count=data["anzahl_retries"],
                average_processing_time_ms=data["durchschnittliche_bearbeitungszeit_ms"],
                details=json.dumps(data, ensure_ascii=False),
            ),
        )

    # ── Issue #16 (umbenannt: ReturnResult, Elena-kompatibel) ─────────────────

    def ReturnResult(self, request, context):
        """
        RESULT_RETURN: Worker → Dispatcher  (Elena: ReturnResult)
        Worker meldet Ergebnis per status="COMPLETED" oder status="FAILED".
        Terminale Tasks werden ignoriert (Idempotenz §5).
        task_id kommt als int32, wird intern als string gespeichert.
        """
        rid       = request.request_id
        task_id   = str(request.payload.task_id)    # int32 → string
        worker_id = request.payload.worker_id
        status    = request.payload.status

        task = self._store.get(task_id)
        if task is None:
            log_event(logger, "warning", "RESULT_RETURN_unknown_task",
                      request_id=rid, task_id=task_id, worker_id=worker_id)
            return taskgrid_pb2.Ack(
                payload=taskgrid_pb2.Ack.Payload(success=False, message="unknown task_id"),
            )

        if is_terminal(task):
            log_event(logger, "warning", "RESULT_RETURN_late",
                      request_id=rid, task_id=task_id,
                      worker_id=worker_id, status=task.status.value)
            return taskgrid_pb2.Ack(
                payload=taskgrid_pb2.Ack.Payload(success=True, message="already terminal"),
            )

        if self._dispatch_loop is not None:
            self._dispatch_loop.cancel_timeout(task_id)

        new_state = TaskState.COMPLETED if status == "COMPLETED" else TaskState.FAILED
        try:
            transition(task, new_state)
        except InvalidTransitionError as e:
            log_event(logger, "error", "RESULT_RETURN_invalid_transition",
                      request_id=rid, task_id=task_id, error=str(e))
            return taskgrid_pb2.Ack(
                payload=taskgrid_pb2.Ack.Payload(success=False, message=str(e)),
            )

        task.result = request.payload.result if status == "COMPLETED" else request.payload.error
        self._store.update(task)

        duration_ms = (
            (task.timestamp_completed - task.timestamp_dispatched) * 1000
            if task.timestamp_dispatched > 0 else 0
        )
        log_event(logger, "info", "RESULT_RETURN_stored",
                  request_id=rid,
                  task_id=task_id,
                  worker_id=worker_id,
                  status=task.status.value,
                  duration_ms=duration_ms)

        return taskgrid_pb2.Ack(
            message_type="ACK",
            request_id=rid,
            timestamp=int(time.time()),
            sender="dispatcher",
            payload=taskgrid_pb2.Ack.Payload(success=True),
        )
