import time
import threading
import grpc
import os
from src.namensdienst.worker import Worker
from proto import taskgrid_pb2, taskgrid_pb2_grpc
from src.common.logger import get_logger, log_event

logger = get_logger("nameservice.nameservice")

NAMESERVICE_UNHEALTHY_SECS  = int(os.environ.get("NAMESERVICE_UNHEALTHY_SECS", 10))
NAMESERVICE_OFFLINE_MULT  = int(os.environ.get("NAMESERVICE_OFFLINE_MULT", 2))

class Namensdienst:
    def __init__(self):
        self.workers = []
        self.idcount = 0

        self.running = False
        self.loopThread = None

        self.startLoop(NAMESERVICE_UNHEALTHY_SECS, NAMESERVICE_OFFLINE_MULT)

    def RegisterWorker(self, request, context):
        payload = request.payload
        if not payload.worker_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("worker_id darf nicht leer sein")
            return taskgrid_pb2.Ack(payload=taskgrid_pb2.Ack.Payload(success=False, message="worker_id missing"))

        self.workers = [
            worker for worker in self.workers
            if not (worker.worker_id == payload.worker_id and worker.address == payload.address and worker.port == payload.port)
        ]

        task_types = list(payload.task_types) or ["unknown"]
        for task_type in task_types:
            worker = Worker(
                task_type,
                payload.address,
                payload.port,
                self.idcount,
                self,
                worker_id=payload.worker_id,
                task_types=task_types,
            )
            worker.lastHeartbeat = time.time()
            worker.currentLoad = payload.current_load
            worker.status = payload.status or "ACTIVE"
            self.workers.append(worker)
            self.idcount += 1

        print(f"Registered worker {payload.worker_id} at {payload.address}:{payload.port} for {', '.join(task_types)}")
        return taskgrid_pb2.Ack(payload=taskgrid_pb2.Ack.Payload(success=True, message="Worker registered"))

    def LookupWorker(self, request, context):
        task_type = request.payload.task_type
        result = [
            worker for worker in self.workers
            if worker.type == task_type and worker.status not in ("UNHEALTHY", "OFFLINE")
        ]

        print(f"Found {len(result)} workers of type {task_type}")
        #log_event(logger, "warning", "NAMESERVICE_lookup_empty", task_type=task_type)
        logger.info(f"Found {len(result)} workers of type {task_type}")
        for worker in result:
            print(f"Worker {worker.worker_id}: {worker.address}, {worker.port}")

        return taskgrid_pb2.LookupResponse(
            payload=taskgrid_pb2.LookupResponse.Payload(
                found=bool(result),
                workers=[
                    taskgrid_pb2.LookupResponse.WorkerPayload(
                        worker_id=worker.worker_id,
                        task_types=worker.task_types,
                        address=worker.address,
                        port=int(worker.port),
                        status=worker.status,
                        current_load=worker.currentLoad,
                    )
                    for worker in result
                ],
                message="Workers found" if result else "No workers found",
            )
        )

    def DeregisterWorker(self, request, context):
        payload = request.payload
        removed = [worker for worker in self.workers if worker.worker_id == payload.worker_id]
        self.workers = [worker for worker in self.workers if worker.worker_id != payload.worker_id]

        print(f"De-Registered worker {payload.worker_id}: removed {len(removed)} entries")
        return taskgrid_pb2.Ack(payload=taskgrid_pb2.Ack.Payload(success=bool(removed), message="Worker deregistered" if removed else "Worker not found"))

    def SendHeartbeat(self, request, context):
        payload = request.payload
        if not payload.worker_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("worker_id darf nicht leer sein")
            return taskgrid_pb2.Ack(payload=taskgrid_pb2.Ack.Payload(success=False, message="worker_id missing"))

        for worker in self.workers:
            if worker.worker_id == payload.worker_id:
                worker.lastHeartbeat = time.time()
                worker.currentLoad = payload.current_load
                worker.status = payload.status or "ACTIVE"
                return taskgrid_pb2.Ack(payload=taskgrid_pb2.Ack.Payload(success=True, message="Heartbeat received"))

        return taskgrid_pb2.Ack(payload=taskgrid_pb2.Ack.Payload(success=False, message="Worker not found"))

    def startLoop(self, x, y):
        self.running = True

        def loop():
            while self.running:
                now = time.time()

                # Kopie erstellen, damit während des Iterierens gelöscht werden kann
                for worker in self.workers[:]:
                    elapsed = now - worker.lastHeartbeat

                    if elapsed >= x * y:
                        worker.status = "OFFLINE"
                        self.workers.remove(worker)
                        print(f"Worker {worker.id} wurde OFFLINE gesetzt und entfernt")
                        #log_event(logger, "warning", "NAMESERVICE_worker_offline", worker_id=worker.id)
                        logger.info(f"Worker {worker.id} wurde OFFLINE gesetzt")

                    elif elapsed >= x:
                        worker.status = "UNHEALTHY"
                        print(f"Worker {worker.id} wurde UNHEALTHY gesetzt")
                        #log_event(logger, "warning", "NAMESERVICE_worker_unhealthy", worker_id=worker.id)
                        logger.info(f"Worker {worker.id} wurde UNHEALTHY gesetzt")

                time.sleep(1)

        self.loopThread = threading.Thread(target=loop, daemon=True)
        self.loopThread.start()

    def endLoop(self):
        self.running = False

        if self.loopThread is not None:
            self.loopThread.join()


"""
status
last_heartbeat
current_load

ACTIVE
UNHEALTHY
DRAINING
OFFLINE

AVAILABLE
WORKING
(UNHEALTHY)
OFFLINE

For communication gRPC
"""

"""
TODO:
Antwortinhalte -> Überall nur ack
Nachrichten empfangen? -> Nur noch Heartbeat fehlt
Hintergrundprozess Loop (Worker auf Unhealthy/Offline setzen) ->
Entfernen wenn Offline -> 
Unhealthy/Offline nicht in Suche zurückgeben -> Done



"""