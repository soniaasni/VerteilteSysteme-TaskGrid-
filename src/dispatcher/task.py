"""
Task-Datenstruktur gemäß Aufgabenstellung Abschnitt 10 (task_t).
Spiegelt alle Pflichtfelder 1:1 wider, angepasst auf Python.
"""

import time
from dataclasses import dataclass, field

from src.common.protocol import TaskState

# Längenbeschränkungen aus der Aufgabenstellung (char[n])
MAX_TYPE_LEN    = 32
MAX_PAYLOAD_LEN = 1024
MAX_RESULT_LEN  = 1024
MAX_WORKER_LEN  = 64


@dataclass
class Task:
    task_id:              str
    task_type:            str        # max 32 Zeichen
    payload:              str        # max 1024 Zeichen
    result:               str       = ""
    status:               TaskState = TaskState.CREATED
    timestamp_created:    int       = field(default_factory=lambda: int(time.time()))
    timestamp_dispatched: int       = 0
    timestamp_completed:  int       = 0
    retry_count:          int       = 0
    assigned_worker:      str       = ""   # max 64 Zeichen
