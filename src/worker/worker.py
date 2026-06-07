import os
import time
import uuid
import grpc
import threading
from concurrent import futures
import signal
import sys

from proto import taskgrid_pb2
from proto import taskgrid_pb2_grpc

from src.worker.task_handlers import hash_handler, reverse, sum_handler, upper
from src.worker.task_handlers import wait_handler

import logging

# ?
class TaskProcessingError(Exception):
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

current_load = 0
load_lock = threading.Lock()

is_draining = False

SHUTDOWN_TIMEOUT = int(os.getenv("SHUTDOWN_TIMEOUT", "10"))


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

HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "5"))


HANDLERS = {
    "reverse": reverse.handle,
    "upper": upper.handle,
    "sum": sum_handler.handle,
    "hash": hash_handler.handle,
    "wait": wait_handler.handle,
}


# Registrierung mit 5 Versuchen mit längerem warten nach jedem Versuch
def register_worker(max_retries=5):
    wait_seconds = 1

    for attempt in range(1, max_retries + 1):

        try:
            with grpc.insecure_channel(NAMING_SERVICE_ADDRESS) as channel:

                stub = taskgrid_pb2_grpc.NamingServiceStub(channel)

                response = stub.RegisterWorker(
                    taskgrid_pb2.RegisterWorkerRequest(
                        message_type="REGISTER_WORKER",
                        request_id=str(uuid.uuid4()),
                        timestamp=int(time.time()),
                        sender=WORKER_ID,
                        payload=taskgrid_pb2.RegisterWorkerRequest.Payload(
                            worker_id=WORKER_ID,
                            task_types=TASK_TYPES,
                            address=WORKER_HOST,
                            port=WORKER_PORT,
                            status="ACTIVE",
                            current_load=get_current_load(),
                        )
                    )
                    )

                if response.payload.success:

                    logging.info(
                        f"worker_id={WORKER_ID} "
                        f"event=REGISTER_SUCCESS "
                        f"attempt={attempt}"
                    )

                    return

                logging.warning(
                    f"worker_id={WORKER_ID} "
                    f"event=REGISTER_REJECTED "
                    f"attempt={attempt} "
                    f"message='{response.payload.message}'"
                )

        except Exception as error:

            logging.error(
                f"worker_id={WORKER_ID} "
                f"event=REGISTER_FAILED "
                f"attempt={attempt}/{max_retries} "
                f"error='{error}'"
            )

        time.sleep(wait_seconds)
        wait_seconds *= 2

    logging.critical(
        f"worker_id={WORKER_ID} "
        f"event=REGISTER_ABORTED "
        f"reason='max retries exceeded'"
    )

    raise RuntimeError(
        f"Worker {WORKER_ID} could not register at naming service."
    )


def send_heartbeat_loop():
    while True:
        try:
            with grpc.insecure_channel(NAMING_SERVICE_ADDRESS) as channel:
                stub = taskgrid_pb2_grpc.NamingServiceStub(channel)

                response = stub.SendHeartbeat(
                    taskgrid_pb2.HeartbeatRequest(
                        message_type="HEARTBEAT",
                        sender = WORKER_ID,
                        timestamp=int(time.time()),
                        request_id = str(uuid.uuid4()),
                        payload = taskgrid_pb2.HeartbeatRequest.Payload(
                            worker_id=WORKER_ID,
                            current_load=get_current_load()
                        )
    )
)

                print(f"[{WORKER_ID}] heartbeat: {response.payload.message}")

        except Exception as error:
            print(f"[{WORKER_ID}] heartbeat failed: {error}")

        time.sleep(HEARTBEAT_INTERVAL)


def deregister_worker():
    try:
        with grpc.insecure_channel(NAMING_SERVICE_ADDRESS) as channel:
            stub = taskgrid_pb2_grpc.NamingServiceStub(channel)

            response = stub.DeregisterWorker(
                taskgrid_pb2.DeregisterWorkerRequest(
                    message_type = "DEREGISTER WORKER",
                    request_id = str(uuid.uuid4()),
                    timestamp=int(time.time()),
                    sender = WORKER_ID,
                    payload = taskgrid_pb2.DeregisterWorkerRequest.Payload(
                        worker_id=WORKER_ID,
                    )
                )
            )

            logging.info(
                f"[{WORKER_ID}] event=DEREGISTER_WORKER "
                f"success={response.payload.success} message='{response.payload.message}'"
            )

    except Exception as error:
        logging.error(
            f"[{WORKER_ID}] event=DEREGISTER_WORKER_FAILED error='{error}'"
        )


def process_task(task):
    if task.payload.task_type not in TASK_TYPES:
        raise TaskProcessingError(
            f"Tasktyp '{task.payload.task_type}' wird von Worker {WORKER_ID} nicht unterstützt"
        )
    if task.payload.task_type not in HANDLERS:
        raise TaskProcessingError(
            f"Kein Handler für Tasktyp '{task.payload.task_type}' implementiert"
        )
    try:
        return HANDLERS[task.payload.task_type](task.payload.task_payload)

    except TaskProcessingError:
        raise

    except Exception as error:
        raise TaskProcessingError(
            f"Interner Fehler bei task_id={task.payload.task_id}: {error}"
        )

def send_result_to_dispatcher(task, result, status="COMPLETED", error=""):
    with grpc.insecure_channel(DISPATCHER_ADDRESS) as channel:
        stub = taskgrid_pb2_grpc.DispatcherServiceStub(channel)

        response = stub.ReturnResult(
            taskgrid_pb2.ResultRequest(
                message_type="RESULT_RETURN",
                request_id=task.request_id,
                timestamp=int(time.time()),
                sender=WORKER_ID,
                payload=taskgrid_pb2.ResultRequest.Payload(
                    task_id=task.payload.task_id,
                    worker_id=WORKER_ID,
                    status=status,
                    result=result,
                    error=error,
                )
            )
        )

        logging.info(
            f"[{WORKER_ID}] response={response.payload.message} "
            f"task_id={task.payload.task_id} status=RESULT_SENT"
        )

def increase_load():
    global current_load

    with load_lock:
        current_load += 1


def decrease_load():
    global current_load

    with load_lock:
        current_load -= 1

def get_current_load():
    with load_lock:
        return current_load
    
def handle_shutdown_signal(signum, _frame):
    global is_draining

    logging.info(
        f"[{WORKER_ID}] event=SHUTDOWN_SIGNAL_RECEIVED signal={signum}"
    )

    is_draining = True
    graceful_shutdown()

def wait_for_running_tasks():
    start_time = time.time()

    while get_current_load() > 0:
        if time.time() - start_time > SHUTDOWN_TIMEOUT:
            logging.warning(
                f"[{WORKER_ID}] event=SHUTDOWN_TIMEOUT "
                f"current_load={get_current_load()}"
            )
            break

        logging.info(
            f"[{WORKER_ID}] event=WAITING_FOR_TASKS "
            f"current_load={get_current_load()}"
        )

        time.sleep(1)

def update_worker_status(status):
    try:
        with grpc.insecure_channel(NAMING_SERVICE_ADDRESS) as channel:
            stub = taskgrid_pb2_grpc.NamingServiceStub(channel)

            response = stub.RegisterWorker(
                taskgrid_pb2.RegisterWorkerRequest(
                    message_type="REGISTER_WORKER",
                    request_id=str(uuid.uuid4()),
                    timestamp=int(time.time()),
                    sender=WORKER_ID,
                    payload=taskgrid_pb2.RegisterWorkerRequest.Payload(
                        worker_id=WORKER_ID,
                        task_types=TASK_TYPES,
                        address=WORKER_HOST,
                        port=WORKER_PORT,
                        status=status,
                        current_load=get_current_load(),
                    )
                )
            )

            logging.info(
                f"[{WORKER_ID}] event=STATUS_UPDATE "
                f"status={status} message='{response.payload.message}'"
            )

    except Exception as error:
        logging.error(
            f"[{WORKER_ID}] event=STATUS_UPDATE_FAILED "
            f"status={status} error='{error}'"
        )

def graceful_shutdown():
    logging.info(f"[{WORKER_ID}] event=GRACEFUL_SHUTDOWN_STARTED")

    update_worker_status("DRAINING")

    wait_for_running_tasks()

    deregister_worker()

    logging.info(f"[{WORKER_ID}] event=GRACEFUL_SHUTDOWN_FINISHED")

    sys.exit(0)

class WorkerService(taskgrid_pb2_grpc.WorkerServiceServicer):

    def ExecuteTask(self, request, context):
        task_id = request.payload.task_id
        task_type = request.payload.task_type

        if is_draining:
            logging.warning(
                f"[{WORKER_ID}] request_id={request.request_id} "
                f"task_id={task_id} status=REJECTED reason=DRAINING"
            )

            return taskgrid_pb2.TaskResponse(
                message_type="TASK_RESPONSE",
                request_id=request.request_id,
                timestamp=int(time.time()),
                sender=WORKER_ID,
                payload=taskgrid_pb2.TaskResponse.Payload(
                    accepted=False,
                    task_id=task_id,
                    worker_id=WORKER_ID,
                    status="rejected",
                    error="Worker is shutting down",
                )
            )

        increase_load()

        logging.info(
            f"[{WORKER_ID}] request_id={request.request_id} "
            f"task_id={task_id} status=PROCESSING"
        )

        try:
            result = process_task(request)
            send_result_to_dispatcher(request, result)

            return taskgrid_pb2.TaskResponse(
                message_type="TASK_RESPONSE",
                request_id=request.request_id,
                timestamp=int(time.time()),
                sender=WORKER_ID,
                payload=taskgrid_pb2.TaskResponse.Payload(
                    accepted=True,
                    task_id=task_id,
                    worker_id=WORKER_ID,
                    status="accepted",
                    error="",
                )
            )

        except Exception as error:
            error_text = (
                f"task_id={task_id} worker_id={WORKER_ID} "
                f"error='{str(error)}'"
            )

            logging.error(
                f"[{WORKER_ID}] request_id={request.request_id} "
                f"task_id={task_id} status=FAILED error='{str(error)}'"
            )

            send_result_to_dispatcher(
                task=request,
                result="",
                status="FAILED",
                error=error_text,
            )

            return taskgrid_pb2.TaskResponse(
                message_type="TASK_RESPONSE",
                request_id=request.request_id,
                timestamp=int(time.time()),
                sender=WORKER_ID,
                payload=taskgrid_pb2.TaskResponse.Payload(
                    accepted=False,
                    task_id=task_id,
                    worker_id=WORKER_ID,
                    status="failed",
                    error=error_text,
                )
            )

        finally:
            decrease_load()


def serve():
    #Wenn Docker beendet wird 
    signal.signal(signal.SIGTERM, handle_shutdown_signal) # docker stop, docker compose stop
    signal.signal(signal.SIGINT, handle_shutdown_signal) # Strg+C

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

    logging.info(
        f"[{WORKER_ID}]"
        f"address={WORKER_HOST}:{WORKER_PORT} types={TASK_TYPES} status=WORKER STARTED"
    )    

    server.wait_for_termination()
    


if __name__ == "__main__":
    serve()