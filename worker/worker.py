import os
import time
import uuid
import grpc
import threading
from concurrent import futures

from generated import taskgrid_pb2
from generated import taskgrid_pb2_grpc

from task_handlers import reverse, upper, sum_handler, hash_handler, wait_handler


#erzeugt eindeutige uuid für jeden Worker (z.B. für neue Worker während der Laufzeit) 
#übersichtlicher für Logs: Bei Systemstart startet Docker compose feste Anzahl von Workern mit fester ID
WORKER_ID = os.getenv("WORKER_ID", f"worker-{uuid.uuid4()}") 
WORKER_HOST = os.getenv("WORKER_HOST", "worker") # muss Docker-Compose-Servicename sein damit Dispatcher den Worker erreichen kann
WORKER_PORT = int(os.getenv("WORKER_PORT", "50051")) # Port nur im Container

# Adressen in Docker-compose hinterlegt
NAMING_SERVICE_ADDRESS = os.getenv("NAMING_SERVICE_ADDRESS", "naming-service:50050")
DISPATCHER_ADDRESS = os.getenv("DISPATCHER_ADDRESS", "dispatcher:50052")

TASK_TYPES = [
    task_type.strip()
    for task_type in os.getenv("TASK_TYPES", "reverse").split(",")
]

HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "5"))


HANDLERS = {
    "reverse": reverse.handle,
    "upper": upper.handle,
    "sum": sum_handler.handle,
    "hash": hash_handler.handle,
    "wait": wait_handler.handle,
}


def register_worker(max_retries=5):
    wait_seconds = 1

    for attempt in range(1, max_retries + 1):
        try:
            with grpc.insecure_channel(NAMING_SERVICE_ADDRESS) as channel:
                stub = taskgrid_pb2_grpc.NamingServiceStub(channel)

                response = stub.RegisterWorker(
                    taskgrid_pb2.WorkerInfo(
                        worker_id=WORKER_ID,
                        task_types=TASK_TYPES,
                        address=WORKER_HOST,
                        port=WORKER_PORT,
                        status="ACTIVE",
                        current_load=0,
                    )
                )

                if response.success:
                    print(f"[{WORKER_ID}] registered: {response.message}")
                    return

                print(f"[{WORKER_ID}] registration rejected: {response.message}")

        except Exception as error:
            print(
                f"[{WORKER_ID}] registration failed "
                f"attempt={attempt}/{max_retries} error={error}"
            )

        time.sleep(wait_seconds)
        wait_seconds *= 2

    raise RuntimeError(f"[{WORKER_ID}] could not register after {max_retries} attempts")


def send_heartbeat_loop():
    while True:
        try:
            with grpc.insecure_channel(NAMING_SERVICE_ADDRESS) as channel:
                stub = taskgrid_pb2_grpc.NamingServiceStub(channel)

                response = stub.SendHeartbeat(
                    taskgrid_pb2.HeartbeatRequest(
                        worker_id=WORKER_ID,
                        timestamp=int(time.time()),
                        current_load=0,
                    )
                )

                print(f"[{WORKER_ID}] heartbeat: {response.message}")

        except Exception as error:
            print(f"[{WORKER_ID}] heartbeat failed: {error}")

        time.sleep(HEARTBEAT_INTERVAL)


def deregister_worker():
    try:
        with grpc.insecure_channel(NAMING_SERVICE_ADDRESS) as channel:
            stub = taskgrid_pb2_grpc.NamingServiceStub(channel)

            response = stub.DeregisterWorker(
                taskgrid_pb2.WorkerInfo(
                    worker_id=WORKER_ID,
                    task_types=TASK_TYPES,
                    address=WORKER_HOST,
                    port=WORKER_PORT,
                    status="OFFLINE",
                    current_load=0,
                )
            )

            print(f"[{WORKER_ID}] deregister_worker: {response.message}")

    except Exception as error:
        print(f"[{WORKER_ID}] deregister failed: {error}")


def process_task(task):
    if task.task_type not in TASK_TYPES:
        raise ValueError(f"Worker unterstützt Tasktyp nicht: {task.task_type}")

    if task.task_type not in HANDLERS:
        raise ValueError(f"Kein Handler implementiert für: {task.task_type}")

    return HANDLERS[task.task_type](task.task_payload)


def send_result_to_dispatcher(task, result, status="COMPLETED", error=""):
    with grpc.insecure_channel(DISPATCHER_ADDRESS) as channel:
        stub = taskgrid_pb2_grpc.DispatcherServiceStub(channel)

        response = stub.ReturnResult(
            taskgrid_pb2.ResultRequest(
                request_id=task.request_id,
                task_id=task.task_id,
                worker_id=WORKER_ID,
                status=status,
                result=result,
                error=error,
            )
        )

        print(
            f"[{WORKER_ID}] result sent "
            f"task_id={task.task_id} status={status} response={response.message}"
        )


class WorkerService(taskgrid_pb2_grpc.WorkerServiceServicer):

    def ExecuteTask(self, request, context):
        print(
            f"[{WORKER_ID}] received task "
            f"request_id={request.request_id} "
            f"task_id={request.task_id} "
            f"type={request.task_type}"
        )

        try:
            result = process_task(request)
            send_result_to_dispatcher(request, result)

            return taskgrid_pb2.TaskResponse(
                status="accepted",
                task_id=request.task_id,
                worker_id=WORKER_ID,
                error="",
            )

        except Exception as error:
            error_text = str(error)

            print(
                f"[{WORKER_ID}] processing failed "
                f"task_id={request.task_id} error={error_text}"
            )

            send_result_to_dispatcher(
                task=request,
                result="",
                status="FAILED",
                error=error_text,
            )

            return taskgrid_pb2.TaskResponse(
                status="failed",
                task_id=request.task_id,
                worker_id=WORKER_ID,
                error=error_text,
            )


def serve():
    register_worker()

    heartbeat_thread = threading.Thread(
        target=send_heartbeat_loop,
        daemon=True,
    )
    heartbeat_thread.start()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    taskgrid_pb2_grpc.add_WorkerServiceServicer_to_server(
        WorkerService(),
        server,
    )

    server.add_insecure_port(f"0.0.0.0:{WORKER_PORT}")
    server.start()

    print(
        f"[{WORKER_ID}] Worker started "
        f"address={WORKER_HOST}:{WORKER_PORT} types={TASK_TYPES}"
    )

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        deregister_worker()
        server.stop(0)


if __name__ == "__main__":
    serve()