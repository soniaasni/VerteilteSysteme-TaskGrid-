"""
Gemeinsames Protokoll-Modul: Nachrichtentypen, Serialisierung, Basisklassen.
Alle Komponenten importieren von hier — kein dupliziertes Format-Wissen.
"""

import json
import uuid
import time
from enum import Enum


class MessageType(str, Enum):
    POST_TASK         = "POST_TASK"
    GET_RESULT        = "GET_RESULT"
    RESULT_RETURN     = "RESULT_RETURN"
    REGISTER_WORKER   = "REGISTER_WORKER"
    HEARTBEAT         = "HEARTBEAT"
    LOOKUP_WORKER     = "LOOKUP_WORKER"
    DEREGISTER_WORKER = "DEREGISTER_WORKER"
    GET_STATUS        = "GET_STATUS"


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


def build_message(message_type: MessageType, sender: str, payload: dict, request_id: str = None) -> bytes:
    """Baut eine UDP-Nachricht als JSON-Bytes zusammen."""
    msg = {
        "message_type": message_type.value,
        "request_id":   request_id or str(uuid.uuid4()),
        "timestamp":    int(time.time()),
        "sender":       sender,
        "payload":      payload,
    }
    return json.dumps(msg).encode("utf-8")


def parse_message(data: bytes) -> dict:
    """Parst empfangene UDP-Bytes zu einem Dict. Wirft ValueError bei ungültigem Format."""
    try:
        msg = json.loads(data.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise ValueError(f"Ungültige Nachricht: {e}") from e

    required = {"message_type", "request_id", "timestamp", "sender", "payload"}
    missing = required - msg.keys()
    if missing:
        raise ValueError(f"Pflichtfelder fehlen: {missing}")

    return msg
