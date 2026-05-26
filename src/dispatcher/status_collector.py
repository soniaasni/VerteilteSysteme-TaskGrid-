"""
Aggregiert Monitoring-Daten aus TaskStore und Namensdienst für GET_STATUS (Issue #20).
"""

from __future__ import annotations

from src.common.protocol import TaskState
from src.dispatcher.task_store import TaskStore
from src.dispatcher.namensdienst_client import NamensdienstClient


class StatusCollector:

    def __init__(self, store: TaskStore, ns_client: NamensdienstClient) -> None:
        self._store     = store
        self._ns_client = ns_client

    def get_status(self) -> dict:
        tasks = self._store.all_tasks()

        queued    = sum(1 for t in tasks if t.status == TaskState.QUEUED)
        running   = sum(1 for t in tasks if t.status in (
                        TaskState.DISPATCHED, TaskState.PROCESSING, TaskState.RETRYING))
        completed = sum(1 for t in tasks if t.status == TaskState.COMPLETED)
        failed    = sum(1 for t in tasks if t.status in (TaskState.FAILED, TaskState.TIMEOUT))
        timeouts  = sum(t.retry_count for t in tasks)

        done_with_time = [
            t for t in tasks
            if t.status == TaskState.COMPLETED
            and t.timestamp_dispatched > 0
            and t.timestamp_completed > 0
        ]
        if done_with_time:
            avg_ms = round(
                sum((t.timestamp_completed - t.timestamp_dispatched) * 1000
                    for t in done_with_time) / len(done_with_time),
                1,
            )
        else:
            avg_ms = 0.0

        known_types = list({t.task_type for t in tasks})

        active_worker_ids: set[str] = set()
        for tt in known_types:
            for w in self._ns_client.lookup_worker(tt):
                active_worker_ids.add(w.worker_id)

        return {
            "aktive_worker":                     len(active_worker_ids),
            "unterstützte_tasktypen":             known_types,
            "offene_tasks":                       queued,
            "laufende_tasks":                     running,
            "abgeschlossene_tasks":               completed,
            "fehlgeschlagene_tasks":              failed,
            "durchschnittliche_bearbeitungszeit_ms": avg_ms,
            "anzahl_timeouts":                    timeouts,
            "anzahl_retries":                     timeouts,
        }
