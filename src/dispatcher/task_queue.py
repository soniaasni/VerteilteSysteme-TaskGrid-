"""
Thread-sichere Task-Warteschlange (FIFO).
Kapselt queue.Queue — Issue #21 kann hier Priorisierung oder
andere Strategien ergänzen ohne den Rest des Dispatchers zu ändern.
"""

import queue
from typing import Optional

from src.dispatcher.task import Task


class TaskQueue:
    def __init__(self) -> None:
        self._q: queue.Queue[Task] = queue.Queue()

    def enqueue(self, task: Task) -> None:
        self._q.put(task)

    def dequeue(self, timeout: float = 1.0) -> Optional[Task]:
        """Gibt nächsten Task zurück oder None bei Timeout."""
        try:
            return self._q.get(timeout=timeout)
        except queue.Empty:
            return None

    def size(self) -> int:
        return self._q.qsize()

    def task_done(self) -> None:
        """Muss nach Verarbeitung eines dequeue()-Tasks aufgerufen werden."""
        self._q.task_done()
