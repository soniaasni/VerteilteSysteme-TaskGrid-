"""
Gemeinsame Typen und Konstanten für alle Komponenten.
Serialisierung übernimmt gRPC/Protobuf — dieses Modul definiert nur
die Zustands-Enums und Hilfsfunktionen die protokollunabhängig sind.
"""

import threading
import uuid
from enum import Enum

# Thread-sicherer Zähler für task_id (int32-kompatibel mit Elenas Worker-Proto)
_task_id_lock = threading.Lock()
_task_id_counter = 0


class TaskState(str, Enum):
    CREATED    = "CREATED"
    QUEUED     = "QUEUED"
    DISPATCHED = "DISPATCHED"
    PROCESSING = "PROCESSING"
    COMPLETED  = "COMPLETED"
    FAILED     = "FAILED"
    TIMEOUT    = "TIMEOUT"
    RETRYING   = "RETRYING"


class WorkerState(str, Enum):
    ACTIVE    = "ACTIVE"
    UNHEALTHY = "UNHEALTHY"
    DRAINING  = "DRAINING"
    OFFLINE   = "OFFLINE"


def new_request_id() -> str:
    return str(uuid.uuid4())


def new_task_id() -> str:
    """Gibt eine eindeutige task_id als String zurück.
    Intern als int32-kompatibler Zähler — für gRPC-Kompatibilität mit Elenas Worker-Proto.
    """
    global _task_id_counter
    with _task_id_lock:
        _task_id_counter += 1
        return str(_task_id_counter)
