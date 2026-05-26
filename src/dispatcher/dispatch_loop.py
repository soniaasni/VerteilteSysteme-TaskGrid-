"""
Dispatch-Loop: Background-Thread der kontinuierlich Tasks aus der Queue
holt und an Worker weiterleitet.

Ablauf pro Task:
  1. Task aus Queue dequeuen
  2. Aktuellen Zustand prüfen (muss QUEUED sein)
  3. lookup_worker() → Worker per Round-Robin auswählen
  4. Falls kein Worker: kurz warten, Task re-enqueuen
  5. QUEUED → DISPATCHED (mit assigned_worker + timestamp_dispatched)
  6. Timeout-Timer starten (Callback-Hook für Issue #18)
  7. DispatchTask gRPC an Worker senden
  8. Bei ACK ok=True:  DISPATCHED → PROCESSING
     Bei ACK ok=False: DISPATCHED → FAILED, Timer abbrechen

Nebenläufigkeit:
  - Main-Loop dequeued sequenziell (keine Race auf Queue.get)
  - Jeder Dispatch-Aufruf läuft in eigenem Thread (ThreadPoolExecutor)
  - Dadurch können mehrere Tasks gleichzeitig in-flight sein
"""

import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from src.common.logger import get_logger, log_event
from src.common.protocol import TaskState
from src.dispatcher.namensdienst_client import NamensdienstClient
from src.dispatcher.state_machine import InvalidTransitionError, is_terminal, transition
from src.dispatcher.task import Task
from src.dispatcher.task_queue import TaskQueue
from src.dispatcher.task_store import TaskStore
from src.dispatcher.worker_client import WorkerClient
from src.dispatcher.worker_selector import RoundRobinSelector

logger = get_logger("dispatcher.dispatch_loop")

_DEFAULT_TIMEOUT_SECS  = int(os.environ.get("DISPATCH_TIMEOUT_SECONDS", "30"))
_NO_WORKER_RETRY_SECS  = float(os.environ.get("DISPATCH_NO_WORKER_RETRY_SECONDS", "2"))


class DispatchLoop(threading.Thread):

    def __init__(
        self,
        store:         TaskStore,
        queue:         TaskQueue,
        ns_client:     NamensdienstClient,
        selector:      RoundRobinSelector,
        worker_client: WorkerClient,
        timeout_secs:  int = _DEFAULT_TIMEOUT_SECS,
    ) -> None:
        super().__init__(daemon=True, name="dispatch-loop")
        self._store         = store
        self._queue         = queue
        self._ns_client     = ns_client
        self._selector      = selector
        self._worker_client = worker_client
        self._timeout_secs  = timeout_secs
        self._running       = False
        self._executor      = ThreadPoolExecutor(max_workers=10, thread_name_prefix="dispatch")
        self._timers:       dict[str, threading.Timer] = {}
        self._timers_lock   = threading.Lock()

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        self._running = True
        super().start()
        logger.info(f"DispatchLoop gestartet (timeout={self._timeout_secs}s)")

    def stop(self) -> None:
        self._running = False
        logger.info("DispatchLoop gestoppt")

    # ── Main Loop ─────────────────────────────────────────────────────────────

    def run(self) -> None:
        while self._running:
            task = self._queue.dequeue(timeout=1.0)
            if task is None:
                continue

            # Frischen Zustand aus Store lesen — könnte sich seit Einreihen geändert haben
            fresh = self._store.get(task.task_id)
            if fresh is None or fresh.status != TaskState.QUEUED:
                continue

            self._executor.submit(self._dispatch, fresh)

    # ── Dispatch ──────────────────────────────────────────────────────────────

    def _dispatch(self, task: Task) -> None:
        """Läuft in eigenem Thread aus dem Executor."""

        # Worker über Namensdienst auflösen (nie statisch!)
        workers = self._ns_client.lookup_worker(task.task_type, task.task_id)
        worker  = self._selector.select(task.task_type, workers)

        if worker is None:
            log_event(logger, "warning", "DISPATCH_no_worker",
                      task_id=task.task_id, task_type=task.task_type)
            time.sleep(_NO_WORKER_RETRY_SECS)
            self._queue.enqueue(task)
            return

        # QUEUED → DISPATCHED
        try:
            transition(task, TaskState.DISPATCHED)
        except InvalidTransitionError as e:
            log_event(logger, "error", "DISPATCH_invalid_transition",
                      task_id=task.task_id, error=str(e))
            return

        task.assigned_worker = worker.worker_id
        self._store.update(task)

        log_event(logger, "info", "DISPATCH_sending",
                  task_id=task.task_id,
                  task_type=task.task_type,
                  worker_id=worker.worker_id,
                  status=task.status.value)

        # Timeout-Timer starten
        # Reaktion auf Timeout (DISPATCHED → TIMEOUT → RETRYING) implementiert Issue #18.
        self._start_timeout_timer(task)

        # Task an Worker senden
        success = self._worker_client.dispatch_task(worker.address, worker.port, task)

        if success:
            # Worker hat Task akzeptiert → DISPATCHED → PROCESSING
            self._cancel_timeout(task.task_id)
            try:
                transition(task, TaskState.PROCESSING)
                self._store.update(task)
                log_event(logger, "info", "DISPATCH_accepted",
                          task_id=task.task_id,
                          worker_id=worker.worker_id,
                          status=task.status.value)
            except InvalidTransitionError as e:
                log_event(logger, "error", "DISPATCH_transition_error",
                          task_id=task.task_id, error=str(e))
        else:
            # Worker nicht erreichbar oder hat abgelehnt → DISPATCHED → FAILED
            self._cancel_timeout(task.task_id)
            try:
                transition(task, TaskState.FAILED)
                self._store.update(task)
                log_event(logger, "error", "DISPATCH_worker_unreachable",
                          task_id=task.task_id,
                          worker_id=worker.worker_id,
                          status=task.status.value)
            except InvalidTransitionError as e:
                log_event(logger, "error", "DISPATCH_transition_error",
                          task_id=task.task_id, error=str(e))

    # ── Timeout-Timer ─────────────────────────────────────────────────────────

    def _start_timeout_timer(self, task: Task) -> None:
        def _on_timeout() -> None:
            # Hook für Issue #18 — hier wird DISPATCHED → TIMEOUT → RETRYING implementiert.
            log_event(logger, "warning", "DISPATCH_TIMEOUT_fired",
                      task_id=task.task_id,
                      worker_id=task.assigned_worker)

        timer = threading.Timer(self._timeout_secs, _on_timeout)
        timer.daemon = True
        with self._timers_lock:
            self._timers[task.task_id] = timer
        timer.start()

    def cancel_timeout(self, task_id: str) -> None:
        """
        Öffentliche Methode — wird von Issue #16 (RESULT_RETURN) aufgerufen
        um den Timeout-Timer zu stoppen wenn ein Ergebnis eingetroffen ist.
        """
        self._cancel_timeout(task_id)

    def _cancel_timeout(self, task_id: str) -> None:
        with self._timers_lock:
            timer = self._timers.pop(task_id, None)
        if timer:
            timer.cancel()
