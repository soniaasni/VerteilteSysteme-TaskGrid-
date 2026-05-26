"""
In-Memory-Speicher für alle Tasks des Dispatchers.
Thread-sicher über einen einzigen Lock — alle Zugriffe serialisiert.
"""

import threading
from typing import Dict, List, Optional

from src.dispatcher.task import Task
from src.common.protocol import TaskState


class TaskStore:
    def __init__(self) -> None:
        self._tasks: Dict[str, Task] = {}
        self._lock = threading.Lock()

    def add(self, task: Task) -> None:
        with self._lock:
            self._tasks[task.task_id] = task

    def get(self, task_id: str) -> Optional[Task]:
        with self._lock:
            return self._tasks.get(task_id)

    def update(self, task: Task) -> None:
        """Überschreibt einen bestehenden Task. Wirft KeyError wenn nicht vorhanden."""
        with self._lock:
            if task.task_id not in self._tasks:
                raise KeyError(f"Task {task.task_id} nicht gefunden")
            self._tasks[task.task_id] = task

    def all_tasks(self) -> List[Task]:
        with self._lock:
            return list(self._tasks.values())

    def count_by_status(self, status: TaskState) -> int:
        with self._lock:
            return sum(1 for t in self._tasks.values() if t.status == status)
