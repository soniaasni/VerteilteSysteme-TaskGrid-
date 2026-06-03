"""
Nameservice gRPC-Server — Einstiegspunkt.
Port und Konfiguration über Umgebungsvariablen (für Docker).
"""

import os
import signal
from concurrent import futures

import grpc

from proto import taskgrid_pb2_grpc
from src.common.logger import get_logger
from src.namensdienst.nameservice import Namensdienst

logger = get_logger("dispatcher.server")


def serve() -> None:
    port = os.environ.get("NAMESERVICE_PORT", "50053")

    servicer = Namensdienst()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    taskgrid_pb2_grpc.add_NamingServiceServicer_to_server(servicer, server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()

    logger.info(f"Nameservice gestartet auf Port {port}")

    def _graceful_stop(sig, frame):
        logger.info("Shutdown-Signal empfangen, stoppe Server...")
        server.stop(grace=5)

    signal.signal(signal.SIGTERM, _graceful_stop)
    signal.signal(signal.SIGINT, _graceful_stop)
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
