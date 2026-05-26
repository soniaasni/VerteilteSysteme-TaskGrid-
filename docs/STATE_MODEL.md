# Task-Zustandsmodell — TaskGrid+

**Pflichtdokument gemäß Aufgabenstellung §5**  
**Implementierung:** `src/dispatcher/state_machine.py`

---

## 1. Zustände

| Zustand | Bedeutung |
|---------|-----------|
| `CREATED` | Task wurde vom Client eingereicht, aber noch nicht in die Queue aufgenommen |
| `QUEUED` | Task liegt in der Warteschlange des Dispatchers, wartet auf Zuteilung |
| `DISPATCHED` | Dispatcher hat den Task an einen Worker gesendet, wartet auf Bestätigung |
| `PROCESSING` | Worker hat den Task akzeptiert und verarbeitet ihn gerade |
| `COMPLETED` | Worker hat das Ergebnis erfolgreich zurückgemeldet — **Terminal** |
| `FAILED` | Task ist endgültig fehlgeschlagen (ungültiger Typ, Worker-Fehler, max. Retries erreicht) — **Terminal** |
| `TIMEOUT` | Worker hat innerhalb der Timeout-Frist kein Ergebnis geliefert |
| `RETRYING` | Task wird erneut eingeplant nach Timeout — Retry-Zähler wurde erhöht |

**Terminal** bedeutet: kein weiterer Zustandsübergang möglich. Ein COMPLETED oder FAILED Task
wird nie überschrieben, auch nicht durch verspätete Ergebnisse (Aufgabenpflicht §5).

---

## 2. Erlaubte Übergänge

```
CREATED ──────────────────────────────────────────► QUEUED
                                                        │
                             ┌──────────────────────────┤
                             │                          │
                             ▼                          ▼
                        DISPATCHED                   FAILED (*)
                        │        │
              ┌─────────┘        └─────────┐
              ▼                            ▼
         PROCESSING                     TIMEOUT
         │        │                        │
         ▼        ▼                        ▼
    COMPLETED   FAILED (*)             RETRYING
                                           │         ┌──► FAILED (*)
                                           └─────────┤
                                                     └──► DISPATCHED
```

Vollständige Übergangstabelle:

| Von | Nach | Auslöser |
|-----|------|---------|
| `CREATED` | `QUEUED` | Dispatcher empfängt POST_TASK (Issue #13) |
| `QUEUED` | `DISPATCHED` | Dispatch-Loop wählt Worker aus (Issue #15) |
| `QUEUED` | `FAILED` | Kein Worker verfügbar nach max. Retries (Issue #18) |
| `DISPATCHED` | `PROCESSING` | Worker bestätigt Empfang per gRPC-ACK (Issue #15) |
| `DISPATCHED` | `TIMEOUT` | Timeout-Timer feuert ohne Ergebnis (Issue #18) |
| `DISPATCHED` | `FAILED` | Worker nicht erreichbar beim Senden (Issue #15) |
| `PROCESSING` | `COMPLETED` | Worker sendet RESULT_RETURN mit Ergebnis (Issue #16) |
| `PROCESSING` | `FAILED` | Worker sendet RESULT_RETURN mit Fehler (Issue #16) |
| `TIMEOUT` | `RETRYING` | Dispatcher leitet Retry ein (Issue #18) |
| `RETRYING` | `DISPATCHED` | Erneute Zuteilung an Worker (Issue #18) |
| `RETRYING` | `FAILED` | Max. Retry-Anzahl überschritten (Issue #18) |

(*) `FAILED` ist immer ein möglicher Übergang aus QUEUED, DISPATCHED und RETRYING
    wenn ein unbehebbarer Fehler auftritt.

---

## 3. Welche Komponente löst welchen Übergang aus

| Übergang | Komponente | Mechanismus |
|----------|-----------|-------------|
| CREATED → QUEUED | **Dispatcher** | `PostTask`-Handler (#13) |
| QUEUED → DISPATCHED | **Dispatcher** | `DispatchLoop._dispatch()` (#15) |
| QUEUED → FAILED | **Dispatcher** | Max. Retry-Wartezeit überschritten (#18) |
| DISPATCHED → PROCESSING | **Dispatcher** | Worker-ACK empfangen (#15) |
| DISPATCHED → TIMEOUT | **Dispatcher** | `threading.Timer` feuert (#18) |
| DISPATCHED → FAILED | **Dispatcher** | gRPC-Fehler beim Senden (#15) |
| PROCESSING → COMPLETED | **Dispatcher** | `ReceiveResult`-Handler mit `success=True` (#16) |
| PROCESSING → FAILED | **Dispatcher** | `ReceiveResult`-Handler mit `success=False` (#16) |
| TIMEOUT → RETRYING | **Dispatcher** | Timeout-Handler (#18) |
| RETRYING → DISPATCHED | **Dispatcher** | Erneute Queue-Einplanung (#18) |
| RETRYING → FAILED | **Dispatcher** | `retry_count >= MAX_RETRIES` (#18) |

> Der Worker löst **keinen** Zustandsübergang direkt aus. Er sendet nur Nachrichten
> an den Dispatcher (RESULT_RETURN), der dann den Übergang vornimmt.

---

## 4. Schutz vor ungültigen Übergängen

Die Datei `src/dispatcher/state_machine.py` erzwingt alle Übergänge zentral:

```python
VALID_TRANSITIONS = {
    TaskState.CREATED:    {TaskState.QUEUED},
    TaskState.QUEUED:     {TaskState.DISPATCHED, TaskState.FAILED},
    TaskState.DISPATCHED: {TaskState.PROCESSING, TaskState.TIMEOUT, TaskState.FAILED},
    TaskState.PROCESSING: {TaskState.COMPLETED,  TaskState.FAILED},
    TaskState.TIMEOUT:    {TaskState.RETRYING},
    TaskState.RETRYING:   {TaskState.DISPATCHED,  TaskState.FAILED},
    TaskState.COMPLETED:  set(),   # terminal — keine weiteren Übergänge
    TaskState.FAILED:     set(),   # terminal — keine weiteren Übergänge
}
```

Jeder Übergang läuft über `transition(task, new_state)`:
- Prüft ob `new_state` in `VALID_TRANSITIONS[task.status]`
- Wirft `InvalidTransitionError` bei ungültigem Übergang
- Setzt Timestamps automatisch (`timestamp_dispatched`, `timestamp_completed`)

**Schutz vor verspäteten Ergebnissen (Pflichtanforderung §5):**  
Da `COMPLETED` und `FAILED` terminale Zustände sind (`set()` im Dict), schlägt jeder
Versuch, einen abgeschlossenen Task zu überschreiben, mit `InvalidTransitionError` fehl.
RESULT_RETURN für einen bereits COMPLETED Task wird ignoriert (Issue #44).

---

## 5. Timeout-Verhalten und Retry-Strategie

Nach dem Dispatch wird ein `threading.Timer` gestartet (konfigurierbar via
`DISPATCH_TIMEOUT_SECONDS`, Standard: 30 Sekunden).

**Wenn der Timer feuert (Issue #18 implementiert die Reaktion):**

```
DISPATCHED → TIMEOUT → RETRYING → DISPATCHED   (wenn retry_count < MAX_RETRIES)
                               → FAILED         (wenn retry_count >= MAX_RETRIES)
```

Konfigurierbare Parameter (Umgebungsvariablen):

| Variable | Standard | Bedeutung |
|----------|---------|-----------|
| `DISPATCH_TIMEOUT_SECONDS` | 30 | Sekunden bis Timeout nach Dispatch |
| `MAX_RETRIES` | 3 | Maximale Anzahl Wiederholungen pro Task |

**Retry-Zähler:** `task.retry_count` wird bei jedem RETRYING-Übergang um 1 erhöht.
Bei `retry_count >= MAX_RETRIES` wird der Task direkt auf FAILED gesetzt.

---

## 6. Erneute Einplanung (Re-enqueue)

Ein Task wird in folgenden Fällen erneut eingeplant:

1. **Kein Worker verfügbar** (QUEUED bleibt): Task wird nach `DISPATCH_NO_WORKER_RETRY_SECONDS`
   (Standard: 2s) wieder in die Queue gelegt. `retry_count` wird dabei **nicht** erhöht.

2. **Timeout** (RETRYING): Task wird erneut in die Queue gelegt. `retry_count` wird erhöht.
   Bei Erschöpfung → FAILED.

Ein Task in COMPLETED oder FAILED wird **niemals** erneut eingeplant.
