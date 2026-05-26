"""
Task-Zustandsmaschine des Dispatchers.
Definiert alle erlaubten Übergänge und verhindert ungültige.
Dokumentation der Zustände: docs/adr/STATE_MODEL.md (Issue #4)

Erlaubte Übergänge gemäß Aufgabenstellung §5:
  CREATED    → QUEUED
  QUEUED     → DISPATCHED | FAILED
  DISPATCHED → PROCESSING | TIMEOUT | FAILED
  PROCESSING → COMPLETED  | FAILED
  TIMEOUT    → RETRYING
  RETRYING   → DISPATCHED | FAILED
  COMPLETED  → (terminal)
  FAILED     → (terminal)
"""

import time
from src.common.protocol import TaskState
from src.dispatcher.task import Task


class InvalidTransitionError(Exception):
    pass


# Alle erlaubten Zustandsübergänge
VALID_TRANSITIONS: dict[TaskState, set[TaskState]] = {
    TaskState.CREATED:    {TaskState.QUEUED},
    TaskState.QUEUED:     {TaskState.DISPATCHED, TaskState.FAILED},
    TaskState.DISPATCHED: {TaskState.PROCESSING, TaskState.TIMEOUT, TaskState.FAILED},
    TaskState.PROCESSING: {TaskState.COMPLETED,  TaskState.FAILED},
    TaskState.TIMEOUT:    {TaskState.RETRYING},
    TaskState.RETRYING:   {TaskState.DISPATCHED,  TaskState.FAILED},
    TaskState.COMPLETED:  set(),   # terminal
    TaskState.FAILED:     set(),   # terminal
}


def transition(task: Task, new_state: TaskState) -> Task:
    """
    Wendet einen Zustandsübergang an.
    Wirft InvalidTransitionError wenn der Übergang nicht erlaubt ist.
    Setzt Timestamps automatisch wo passend.
    """
    allowed = VALID_TRANSITIONS.get(task.status, set())
    if new_state not in allowed:
        raise InvalidTransitionError(
            f"Task {task.task_id}: Übergang {task.status} → {new_state} nicht erlaubt. "
            f"Erlaubt: {[s.value for s in allowed] or 'keine (terminal)'}"
        )

    task.status = new_state

    if new_state == TaskState.DISPATCHED:
        task.timestamp_dispatched = int(time.time())
    elif new_state in (TaskState.COMPLETED, TaskState.FAILED):
        task.timestamp_completed = int(time.time())

    return task


def is_terminal(task: Task) -> bool:
    """Gibt True zurück wenn der Task nicht mehr geändert werden darf."""
    return task.status in (TaskState.COMPLETED, TaskState.FAILED)
