# TaskGrid+ — Projektplan

**Prüfung:** Verteilte Systeme · Softwareentwurf  
**Abgabe:** 08.06.2026 EOB  
**Heute:** 21.05.2026 → **18 Tage verbleibend**  
**Dozent:** Kevin Dallmann · Jahrgang TIK23

---

## Bewertungsübersicht (100 Punkte)

| Kriterium | Punkte |
|---|---|
| Client, Dispatcher, Worker – Basisfunktionen | 10 |
| Namensdienst mit Registrierung, Lookup, Deregistrierung | 10 |
| Mehrere Worker pro Tasktyp und Auswahlstrategie | 10 |
| Kommunikation, Nachrichtenformat und Logging | 15 |
| Ergebnisverwaltung, Queue und Task-Zustandsmodell | 15 |
| Robustheit: Heartbeat, Timeout, Retry, Fehlerbehandlung | 15 |
| Modularität und Erweiterbarkeit neuer Tasktypen | 10 |
| Containerisierung und Docker Compose | 5 |
| Dokumentation, Diagramme und Architekturentscheidungen | 10 |
| Tests und Nachweisfunktionalität | 10 |
| **Gesamt** | **100** |

---

## Mindestanforderungen zum Bestehen (PFLICHT)

- [ ] Client kann Aufgaben an Dispatcher senden
- [ ] Dispatcher kann Aufgaben speichern und weiterleiten
- [ ] Mindestens ein Worker verarbeitet Aufgaben
- [ ] **Namensdienst wird tatsächlich verwendet** (kein Bypass!)
- [ ] Keine statischen Worker-Adressen im Dispatcher
- [ ] Ergebnisse können abgefragt werden
- [ ] Docker Compose startet alle wesentlichen Komponenten
- [ ] Dokumentation und Testprotokoll liegen vor

> ACHTUNG: Fehlt der Namensdienst oder wird er nicht vom Dispatcher genutzt → Aufgabe nicht bestanden.

---

## Systemarchitektur — Überblick

```
Client
  │  POST_TASK / GET_RESULT  (UDP oder RPC)
  ▼
Dispatcher ──── LOOKUP_WORKER ────► Namensdienst ◄── REGISTER_WORKER / HEARTBEAT (Worker)
  │                                                              │
  │  GET_TASK / RESULT_RETURN                         DEREGISTER_WORKER (Worker)
  ▼
Worker(s)   [je ein Container pro Typ, mehrere Instanzen möglich]

Monitoring ──── GET_STATUS ────► Dispatcher / Namensdienst
```

### Container-Struktur (docker-compose.yml)
| Container | Pflicht |
|---|---|
| `client` | Ja |
| `dispatcher` | Ja |
| `worker-<typ>` (min. 1, mehrere Instanzen) | Ja |
| `namensdienst` | Ja |
| `monitoring` | Optional |

---

## Phasenplan

### Phase 1 — Architektur & Design  `21.05 – 24.05` (4 Tage)

- [ ] **Kommunikationsprotokoll wählen** (UDP vs. RPC — muss begründet werden!)
  - Aspekte: Zuverlässigkeit, Paketverlust, Timeouts, Wiederholungen, Antwortzuordnung, Serialisierung
- [ ] **Technologiestack festlegen** (Sprache, Bibliotheken)
- [ ] **Architekturdiagramm skizzieren** (Komponenten, Kommunikationswege)
- [ ] **Nachrichtenformat definieren** (JSON empfohlen)
  - Jede Nachricht muss enthalten: `message_type`, `request_id`/`correlation_id`, `timestamp`, `sender`, `payload`
  - Taskbezogen zusätzlich: `task_id`, `task_type`, `task_payload`, `task_status`
- [ ] **Task-Zustandsmodell entwerfen**
  ```
  CREATED → QUEUED → DISPATCHED → PROCESSING → COMPLETED
                                              → FAILED
                     DISPATCHED → TIMEOUT → RETRYING → DISPATCHED
  CREATED → QUEUED → FAILED
  ```
- [ ] **Worker-Zustände definieren:** `ACTIVE`, `UNHEALTHY`, `DRAINING`, `OFFLINE`
- [ ] **Worker-Auswahlstrategie wählen** (Round-Robin, Least-Load, Random — muss begründet werden!)
- [ ] **5+ ADRs vorbereiten** (Architecture Decision Records):
  - ADR-001: UDP vs. RPC
  - ADR-002: Nachrichtenformat (z.B. JSON)
  - ADR-003: Worker-Auswahlstrategie
  - ADR-004: Speicherung von Tasks und Ergebnissen
  - ADR-005: Heartbeat-Mechanismus (Intervall, Timeout-Schwelle)
  - ADR-006: Retry-Strategie

---

### Phase 2 — Implementierung Kern `25.05 – 31.05` (7 Tage)

#### 2.1 Namensdienst (höchste Priorität!)
- [ ] `register_worker(worker_id, type, address, port, capacity?)` → `REGISTER_WORKER`
- [ ] `lookup_worker(type)` → `LOOKUP_WORKER` → Liste von Worker-Instanzen zurückgeben
- [ ] `receive_heartbeat(worker_id)` → `HEARTBEAT`
- [ ] `deregister_worker(address)` → `DEREGISTER_WORKER`
- [ ] Worker-Metadaten verwalten: `worker_id`, `type`, `address`, `port`, `status`, `last_heartbeat`, `current_load`
- [ ] Inaktive Worker erkennen (ausgebliebener Heartbeat → `UNHEALTHY`/`OFFLINE`)
- [ ] Mapping: `type → [Liste von Worker-Instanzen]` (NICHT nur `type → address`!)

#### 2.2 Dispatcher
- [ ] `receive_task()` — POST_TASK entgegennehmen, eindeutige Task-ID vergeben
- [ ] `enqueue_task(task)` — Task in Warteschlange einreihen (Status: `QUEUED`)
- [ ] `lookup_worker(type)` — Worker über Namensdienst zur Laufzeit auflösen (NICHT statisch!)
- [ ] `dispatch_task_to_worker(task)` — Task an Worker senden (Status: `DISPATCHED`)
- [ ] `store_result(task_id, result)` — Ergebnis speichern (Status: `COMPLETED`)
- [ ] `respond_with_result(task_id)` — GET_RESULT beantworten
- [ ] Task-Zustandsverwaltung (alle Übergänge implementieren)
- [ ] Timeout-Handling: Bei ausbleibender Worker-Rückmeldung → Retry oder `FAILED`/`TIMEOUT`
- [ ] Strukturierte Logs erzeugen (mit `request_id`, `task_id`, `status`)
- [ ] Monitoring-Schnittstelle bereitstellen (GET_STATUS)
- [ ] **KEIN statisches Hinterlegen von Worker-Adressen!**

#### 2.3 Worker
- [ ] `register_worker()` — Beim Start beim Namensdienst registrieren
- [ ] `send_heartbeat()` — Regelmäßige Heartbeats an Namensdienst
- [ ] `receive_task()` — Aufgaben vom Dispatcher empfangen
- [ ] `process_task(task)` — Aufgabe verarbeiten (Status: `PROCESSING`)
- [ ] `send_result(task_id, result)` — Ergebnis an Dispatcher senden
- [ ] Bei Shutdown: `DEREGISTER_WORKER` senden
- [ ] Fehler bei der Verarbeitung melden
- [ ] **Worker darf NICHT direkt vom Client angesprochen werden**

#### 2.4 Client
- [ ] `send_task(type, payload)` → POST_TASK → Task-ID empfangen
- [ ] `request_result(task_id)` → GET_RESULT → Ergebnis oder Statusanzeige
- [ ] Fehlerfälle verständlich anzeigen (unbekannter Typ, noch kein Ergebnis, etc.)

#### 2.5 Tasktypen implementieren (min. 3 Pflicht, alle 5 empfohlen)
| Typ | Beschreibung | Priorität |
|---|---|---|
| `reverse` | String umdrehen | PFLICHT |
| `sum` | Zahlen summieren | PFLICHT |
| `hash` | SHA256-Berechnung | PFLICHT |
| `upper` | Großschreibung | empfohlen |
| `wait` | Künstliche Verzögerung | empfohlen |
| `wordcount` | Wörter zählen | optional |
| `lower` | Kleinschreibung | optional |
| `base64` | Base64-Encoding | optional |
| `prime` | Primzahlprüfung | optional |

---

### Phase 3 — Containerisierung `01.06 – 02.06` (2 Tage)

- [ ] **Dockerfile** für Client
- [ ] **Dockerfile** für Dispatcher
- [ ] **Dockerfile** für Worker (parametrisierbar per Tasktyp)
- [ ] **Dockerfile** für Namensdienst
- [ ] **Dockerfile** für Monitoring (optional)
- [ ] **docker-compose.yml** erstellen:
  - Alle Pflichtkomponenten als Services
  - Mehrere Worker-Instanzen desselben Typs startbar: `docker compose up --scale worker-sum=3`
  - Konfiguration über Umgebungsvariablen
  - Volumes NUR für: Logs, persistente Dispatcher-Daten, Testdaten, Dokumentationsartefakte
  - **KEIN** gemeinsames Volume zur Aufgaben-/Ergebnisübergabe zwischen Komponenten
- [ ] Netzwerktopologie definieren (internes Docker-Netzwerk)
- [ ] Logs-Volume einrichten

---

### Phase 4 — Robustheit, Tests & Monitoring `03.06 – 05.06` (3 Tage)

#### Robustheit
- [ ] Heartbeat-Intervall konfigurierbar
- [ ] Worker nach N ausgebliebenen Heartbeats als `OFFLINE` markieren
- [ ] Timeout im Dispatcher: nach Ablauf → Retry oder `FAILED`/`TIMEOUT`
- [ ] Fehlerfall: unbekannter Tasktyp → verständliche Fehlermeldung
- [ ] Fehlerfall: kein verfügbarer Worker → Task in `FAILED` oder warten
- [ ] Fehlerfall: Worker fällt während Bearbeitung aus → Retry auf anderem Worker
- [ ] Fehlerfall: verspätetes Ergebnis → abgeschlossener Task wird nicht überschrieben
- [ ] Fehlerfall: doppelte Ergebnisrückgabe → idempotent behandeln
- [ ] Fehlerfall: ungültige Payload → Fehlermeldung
- [ ] Fehlerfall: Namensdienst temporär nicht erreichbar

#### Monitoring-Schnittstelle (GET_STATUS)
Muss anzeigen: aktive Worker, unterstützte Tasktypen, offene/laufende/abgeschlossene/fehlgeschlagene Tasks, durchschnittliche Bearbeitungszeit, Anzahl Timeouts, Anzahl Retries

#### Tests
**Erfolgreiche Tests (mind. 6, mind. 3 verschiedene Tasktypen):**
- [ ] Test 1: `reverse` — Eingabe, erw. Ergebnis, tats. Ergebnis, Worker-ID, Log-Auszug, Task-ID, Statusverlauf
- [ ] Test 2: `sum` — (wie oben)
- [ ] Test 3: `hash` — (wie oben)
- [ ] Test 4: `upper` — (wie oben)
- [ ] Test 5: `wait` — (wie oben)
- [ ] Test 6: weiterer Typ nach Wahl — (wie oben)

**Fehler- und Robustheitstests (mind. 3):**
- [ ] Fehlertest 1: Worker-Container wird während Bearbeitung gestoppt
- [ ] Fehlertest 2: unbekannter Tasktyp wird gesendet
- [ ] Fehlertest 3: Worker antwortet nicht innerhalb des Timeouts
- [ ] (optional) Fehlertest 4: Namensdienst nicht erreichbar
- [ ] (optional) Fehlertest 5: ungültige Payload

Für jeden Test dokumentieren: Ausgangssituation, durchgeführte Aktion, erwartetes Verhalten, tatsächliches Verhalten, relevante Logs, Bewertung des Systemverhaltens.

---

### Phase 5 — Dokumentation & Abgabe `06.06 – 08.06` (3 Tage)

#### PDF-Dokumentation (mind. folgende 19 Punkte)
- [ ] 1. Architekturdiagramm
- [ ] 2. Begründung der Architektur
- [ ] 3. Beschreibung der Komponenten
- [ ] 4. Kommunikationsprotokoll inkl. Nachrichtenformat
- [ ] 5. Beschreibung von `task_t` und relevanten Datenstrukturen
- [ ] 6. Schnittstellendokumentation (alle 8 Nachrichtentypen)
- [ ] 7. Task-Zustandsmodell inkl. Zustandsübergängen
- [ ] 8. Ablaufdiagramm: Client → Dispatcher → Worker → Dispatcher → Client
- [ ] 9. Fehlerfall-Ablaufdiagramm (Worker-Ausfall oder Timeout)
- [ ] 10. Beschreibung des Namensdienstes
- [ ] 11. Beschreibung der Worker-Registrierung und Heartbeats
- [ ] 12. Beschreibung der Worker-Auswahlstrategie (begründet)
- [ ] 13. Eingesetzte externe Bibliotheken und deren Funktionalität
- [ ] 14. Dokumentation des Buildprozesses
- [ ] 15. Startanleitung
- [ ] 16. Auflistung eingesetzter Technologien
- [ ] 17. Testprotokoll mit Logs und Screenshots
- [ ] 18. Mindestens 5 ADRs (Architecture Decision Records)
- [ ] 19. Quellenangaben

#### Abgabe-Checkliste (git-Repository)
- [ ] Quelltext: `client/`
- [ ] Quelltext: `dispatcher/`
- [ ] Quelltext: `worker/`
- [ ] Quelltext: `namensdienst/`
- [ ] Quelltext: `monitoring/` (oder Schnittstelle im Dispatcher)
- [ ] `client/Dockerfile`
- [ ] `dispatcher/Dockerfile`
- [ ] `worker/Dockerfile`
- [ ] `namensdienst/Dockerfile`
- [ ] `docker-compose.yml`
- [ ] `docs/dokumentation.pdf`
- [ ] `docs/testprotokoll.pdf` (oder in Dokumentation integriert)
- [ ] `README.md` / Startanleitung
- [ ] Konfigurationsdateien (falls vorhanden)
- [ ] Beispieldaten (falls vorhanden)

---

## Schnittstellenreferenz (8 Nachrichtentypen)

| Nachricht | Richtung | Pflichtfelder |
|---|---|---|
| `POST_TASK` | Client → Dispatcher | `type`, `payload` → gibt `task_id` zurück |
| `GET_RESULT` | Client → Dispatcher | `task_id` → gibt `result`, `status`, `error` zurück |
| `RESULT_RETURN` | Worker → Dispatcher | `task_id`, `result`, `worker_id` |
| `REGISTER_WORKER` | Worker → Namensdienst | `worker_id`, `type`, `address`, `port`, `capacity?` |
| `HEARTBEAT` | Worker → Namensdienst | `worker_id`, `timestamp`, `current_load?` |
| `LOOKUP_WORKER` | Dispatcher → Namensdienst | `type` → Liste geeigneter Worker |
| `DEREGISTER_WORKER` | Worker → Namensdienst | `worker_id` |
| `GET_STATUS` | Monitoring → Dispatcher/NS | → Systemstatus |

---

## Zeitplan-Übersicht

```
KW 21  Mo 21.05 ████ Phase 1: Architektur & Design beginnen
       Di 22.05 ████ Protokoll wählen, Nachrichtenformat definieren
       Mi 23.05 ████ Task-Zustandsmodell, Worker-Zustände
       Do 24.05 ████ ADRs beginnen, Architektur finalisieren
KW 22  Fr 25.05 ████ Phase 2: Namensdienst implementieren
       Sa 26.05 ████ Namensdienst + Heartbeat-Logik
       So 27.05 ████ Dispatcher Kern
       Mo 28.05 ████ Dispatcher komplett
       Di 29.05 ████ Worker Basis + Tasktypen
       Mi 30.05 ████ Client + Integration testen
       Do 31.05 ████ Mehrere Worker-Instanzen, Lastverteilung
KW 23  Fr 01.06 ████ Phase 3: Dockerfiles
       Sa 02.06 ████ docker-compose.yml, Skalierung testen
       So 03.06 ████ Phase 4: Robustheit (Timeout, Retry)
       Mo 04.06 ████ Fehlerbehandlung + Monitoring
       Di 05.06 ████ Tests durchführen + dokumentieren (6 OK + 3 Fehler)
KW 24  Mi 06.06 ████ Phase 5: PDF-Dokumentation
       Do 07.06 ████ ADRs fertig, Testprotokoll mit Screenshots
       Fr 08.06 ████ ABGABE (EOB)
```

---

## Kritische Hinweise (nicht vergessen!)

1. **Namensdienst ist PFLICHT** — Docker-internes DNS ersetzt ihn NICHT
2. **Keine statischen Worker-Adressen** im Dispatcher-Code oder Config
3. **Kommunikation ausschließlich über UDP oder RPC** — keine gemeinsamen Volumes für Datenaustausch
4. **Ergebnisse dürfen nicht überschrieben werden** — bereits abgeschlossene Tasks sind immutable
5. **Strukturierte Logs** mit `request_id` und `task_id` in allen Komponenten
6. **Eigenleistung muss erkennbar sein** — KI-Nutzung erlaubt, aber alles muss erklärt werden können
7. **Architekturabnahme** (Elevator Pitch) — Architektur und Designentscheidungen erklären können
