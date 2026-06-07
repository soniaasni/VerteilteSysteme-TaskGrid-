"""
Namensdienst-Client des Dispatchers.
Kapselt alle gRPC-Aufrufe an den Namensdienst — der Rest des Dispatchers
kennt weder Adressen noch Ports von Workern.

Adresse des Namensdiensts kommt ausschließlich aus Umgebungsvariablen:
  NAMENSDIENST_HOST  (default: "namensdienst")
  NAMENSDIENST_PORT  (default: "50052")
"""

import os
import time
from dataclasses import dataclass
from typing import List, Optional

import grpc

from proto import taskgrid_pb2, taskgrid_pb2_grpc
from src.common.logger import get_logger, log_event

logger = get_logger("dispatcher.namensdienst_client")


@dataclass
class WorkerInfo:
    """Metadaten eines vom Namensdienst zurückgegebenen Workers."""
    worker_id:    str
    address:      str
    port:         int
    current_load: int


class NamensdienstClient:
    """
    Thin wrapper um den gRPC-Stub des Namensdiensts.
    Für jeden Lookup wird eine frische Verbindung aufgebaut, damit
    Pausieren/Weiterlaufen des Namensdiensts sauber erneut benutzt werden kann.
    """

    def __init__(self) -> None:
        host = os.environ.get("NAMENSDIENST_HOST", "namensdienst")
        port = os.environ.get("NAMENSDIENST_PORT", "50052")
        self._target = f"{host}:{port}"
        logger.info(f"NamensdienstClient verbindet zu {self._target}")

    def lookup_worker(self, task_type: str, request_id: str = "") -> List[WorkerInfo]:
        """
        LOOKUP_WORKER: Gibt Liste aller ACTIVE Worker für task_type zurück.
        Gibt leere Liste zurück wenn kein Worker verfügbar oder Namensdienst
        nicht erreichbar (kein Absturz — Fehler wird geloggt).
        """
        channel = grpc.insecure_channel(self._target)
        try:
            stub = taskgrid_pb2_grpc.NamingServiceStub(channel)
            response = stub.LookupWorker(
                taskgrid_pb2.LookupRequest(
                    message_type="LOOKUP_WORKER",
                    request_id=request_id,
                    timestamp=int(time.time()),
                    sender="dispatcher",
                    payload=taskgrid_pb2.LookupRequest.Payload(
                        task_type=task_type,
                    ),
                ),
                timeout=5.0,
            )
        except grpc.RpcError as e:
            log_event(logger, "error", "LOOKUP_WORKER_failed",
                      request_id=request_id,
                      task_type=task_type,
                      error=str(e.code()),
                      target=self._target)
            return []
        finally:
            channel.close()

        if not response.payload.found or not response.payload.workers:
            log_event(logger, "warning", "LOOKUP_WORKER_no_workers",
                      request_id=request_id,
                      task_type=task_type)
            return []

        workers = [
            WorkerInfo(
                worker_id=w.worker_id,
                address=w.address,
                port=w.port,
                current_load=w.current_load,
            )
            for w in response.payload.workers
        ]
        log_event(logger, "info", "LOOKUP_WORKER_success",
                  request_id=request_id,
                  task_type=task_type,
                  worker_count=len(workers))
        return workers
