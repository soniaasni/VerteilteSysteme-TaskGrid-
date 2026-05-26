"""
Worker-Auswahlstrategie: Round-Robin.

Entscheidung (ADR-003): Round-Robin wurde gewählt weil
  - einfach und deterministisch
  - verteilt Last gleichmäßig über alle ACTIVE Worker
  - kein globaler Zustand außer einem Index nötig
  - Aufgabenstellung verbietet dauerhaft denselben Worker

Pro Tasktyp wird ein eigener Zähler geführt, damit zwei Typen
sich nicht gegenseitig beeinflussen.
"""

import threading
from collections import defaultdict
from typing import Optional

from src.dispatcher.namensdienst_client import WorkerInfo


class RoundRobinSelector:

    def __init__(self) -> None:
        self._counters: dict[str, int] = defaultdict(int)
        self._lock = threading.Lock()

    def select(self, task_type: str, workers: list[WorkerInfo]) -> Optional[WorkerInfo]:
        """
        Wählt einen Worker per Round-Robin aus der übergebenen Liste.
        Gibt None zurück wenn die Liste leer ist.
        """
        if not workers:
            return None

        with self._lock:
            idx = self._counters[task_type] % len(workers)
            self._counters[task_type] += 1

        return workers[idx]
