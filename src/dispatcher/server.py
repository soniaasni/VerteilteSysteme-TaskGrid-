"""
Dispatcher gRPC-Server — Einstiegspunkt.
Port und Konfiguration über Umgebungsvariablen (für Docker).
"""

import os
import signal
from concurrent import futures

import grpc

from proto import taskgrid_pb2_grpc
from src.common.logger import get_logger
from src.dispatcher.servicer import DispatcherServicer
from src.dispatcher.task_queue import TaskQueue
from src.dispatcher.task_store import TaskStore

logger = get_logger("dispatcher.server")


def serve() -> None:
    port = os.environ.get("DISPATCHER_PORT", "50051")

    store = TaskStore()
    task_queue = TaskQueue()
    servicer = DispatcherServicer(store, task_queue)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    taskgrid_pb2_grpc.add_DispatcherServiceServicer_to_server(servicer, server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()

    logger.info(f"Dispatcher gestartet auf Port {port}")

    def _graceful_stop(sig, frame):
        logger.info("Shutdown-Signal empfangen, stoppe Server...")
        server.stop(grace=5)

    signal.signal(signal.SIGTERM, _graceful_stop)
    signal.signal(signal.SIGINT, _graceful_stop)
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
