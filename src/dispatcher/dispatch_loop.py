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
_DEFAULT_MAX_RETRIES   = int(os.environ.get("MAX_RETRIES", "3"))
_NO_WORKER_RETRY_SECS  = float(os.environ.get("DISPATCH_NO_WORKER_RETRY_SECONDS", "2"))
# Mindestverzögerung vor Re-Enqueue nach Timeout, damit der RETRYING-Zustand
# für externe Beobachter (Polling, Tests) sichtbar bleibt (Issue #18)
_RETRY_REENQUEUE_DELAY_SECS = float(os.environ.get("DISPATCH_RETRY_REENQUEUE_DELAY_SECONDS", "1.0"))


class DispatchLoop(threading.Thread):

    def __init__(
        self,
        store:                    TaskStore,
        queue:                    TaskQueue,
        ns_client:                NamensdienstClient,
        selector:                 RoundRobinSelector,
        worker_client:            WorkerClient,
        timeout_secs:             int   = _DEFAULT_TIMEOUT_SECS,
        max_retries:              int   = _DEFAULT_MAX_RETRIES,
        retry_reenqueue_delay:    float = _RETRY_REENQUEUE_DELAY_SECS,
    ) -> None:
        super().__init__(daemon=True, name="dispatch-loop")
        self._store                  = store
        self._queue                  = queue
        self._ns_client              = ns_client
        self._selector               = selector
        self._worker_client          = worker_client
        self._timeout_secs           = timeout_secs
        self._max_retries            = max_retries
        self._retry_reenqueue_delay  = retry_reenqueue_delay
        self._running                = False
        self._executor               = ThreadPoolExecutor(max_workers=10, thread_name_prefix="dispatch")
        self._timers:                dict[str, threading.Timer] = {}
        self._timers_lock            = threading.Lock()

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
            if fresh is None or fresh.status not in (TaskState.QUEUED, TaskState.RETRYING):
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

        # QUEUED → DISPATCHED  oder  RETRYING → DISPATCHED
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
        # Rückgabewert: True = akzeptiert, False = abgelehnt, None = gRPC-Fehler
        success = self._worker_client.dispatch_task(worker.address, worker.port, task)

        if success is True:
            # Worker hat Task akzeptiert → DISPATCHED → PROCESSING
            # Timeout-Timer läuft weiter bis ReturnResult eintrifft (Issue #18)
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

        elif success is False:
            # Worker hat Task explizit abgelehnt (accepted=False) → sofort FAILED
            # Timer abbrechen, kein weiterer Retry
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

        else:
            # gRPC-Fehler (Timeout/Netzwerk): success is None
            # Timer läuft weiter → DISPATCH_TIMEOUT_fired → RETRYING → ggf. FAILED
            # Kein sofortiges FAILED hier — Timer-Retry-Mechanismus übernimmt (Issue #18)
            log_event(logger, "warning", "DISPATCH_grpc_error_timer_handles",
                      task_id=task.task_id,
                      worker_id=worker.worker_id)

    # ── Timeout-Timer ─────────────────────────────────────────────────────────

    def _start_timeout_timer(self, task: Task) -> None:
        task_id = task.task_id

        def _on_timeout() -> None:
            fresh = self._store.get(task_id)
            if fresh is None or is_terminal(fresh):
                return

            log_event(logger, "warning", "DISPATCH_TIMEOUT_fired",
                      task_id=task_id, worker_id=fresh.assigned_worker,
                      retry_count=fresh.retry_count)

            # DISPATCHED → TIMEOUT → RETRYING
            try:
                transition(fresh, TaskState.TIMEOUT)
                transition(fresh, TaskState.RETRYING)
                fresh.retry_count += 1
                self._store.update(fresh)
            except InvalidTransitionError as e:
                log_event(logger, "error", "TIMEOUT_transition_error",
                          task_id=task_id, error=str(e))
                return

            if fresh.retry_count >= self._max_retries:
                # Max. Retries erschöpft → FAILED
                try:
                    transition(fresh, TaskState.FAILED)
                    self._store.update(fresh)
                    log_event(logger, "error", "TIMEOUT_max_retries_exceeded",
                              task_id=task_id, retry_count=fresh.retry_count,
                              max_retries=self._max_retries)
                except InvalidTransitionError as e:
                    log_event(logger, "error", "TIMEOUT_transition_error",
                              task_id=task_id, error=str(e))
            else:
                # Erneut einplanen nach kurzer Verzögerung.
                # Die Verzögerung stellt sicher, dass der RETRYING-Zustand
                # für externe Beobachter (Monitoring, Polling, Tests) sichtbar bleibt.
                # Ohne Verzögerung würde die Dispatch-Loop den Task innerhalb von
                # Millisekunden abholen und sofort auf DISPATCHED setzen.
                log_event(logger, "info", "TIMEOUT_retrying",
                          task_id=task_id, retry_count=fresh.retry_count,
                          max_retries=self._max_retries)

                def _reenqueue_after_delay(t=fresh) -> None:
                    self._queue.enqueue(t)

                delay_timer = threading.Timer(self._retry_reenqueue_delay, _reenqueue_after_delay)
                delay_timer.daemon = True
                delay_timer.start()

        timer = threading.Timer(self._timeout_secs, _on_timeout)
        timer.daemon = True
        with self._timers_lock:
            self._timers[task_id] = timer
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
