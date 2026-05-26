"""
Namensdienst-Client des Dispatchers.
Kapselt alle gRPC-Aufrufe an den Namensdienst — der Rest des Dispatchers
kennt weder Adressen noch Ports von Workern.

Adresse des Namensdiensts kommt ausschließlich aus Umgebungsvariablen:
  NAMENSDIENST_HOST  (default: "namensdienst")
  NAMENSDIENST_PORT  (default: "50052")
"""

import os
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
    Verbindung wird lazy aufgebaut und bei jedem Aufruf genutzt.
    """

    def __init__(self) -> None:
        host = os.environ.get("NAMENSDIENST_HOST", "namensdienst")
        port = os.environ.get("NAMENSDIENST_PORT", "50052")
        self._target = f"{host}:{port}"
        self._channel = grpc.insecure_channel(self._target)
        self._stub = taskgrid_pb2_grpc.NamensdienstServiceStub(self._channel)
        logger.info(f"NamensdienstClient verbindet zu {self._target}")

    def lookup_worker(self, task_type: str, request_id: str = "") -> List[WorkerInfo]:
        """
        LOOKUP_WORKER: Gibt Liste aller ACTIVE Worker für task_type zurück.
        Gibt leere Liste zurück wenn kein Worker verfügbar oder Namensdienst
        nicht erreichbar (kein Absturz — Fehler wird geloggt).
        """
        try:
            response = self._stub.LookupWorker(
                taskgrid_pb2.LookupRequest(task_type=task_type),
                timeout=5.0,
            )
        except grpc.RpcError as e:
            log_event(logger, "error", "LOOKUP_WORKER_failed",
                      request_id=request_id,
                      task_type=task_type,
                      error=str(e.code()),
                      target=self._target)
            return []

        if not response.found or not response.workers:
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
            for w in response.workers
        ]
        log_event(logger, "info", "LOOKUP_WORKER_success",
                  request_id=request_id,
                  task_type=task_type,
                  worker_count=len(workers))
        return workers
