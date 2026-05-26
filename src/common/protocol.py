"""
Gemeinsame Typen und Konstanten für alle Komponenten.
Serialisierung übernimmt gRPC/Protobuf — dieses Modul definiert nur
die Zustands-Enums und Hilfsfunktionen die protokollunabhängig sind.
"""

import uuid
from enum import Enum


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
    return str(uuid.uuid4())
