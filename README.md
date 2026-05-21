# VerteilteSysteme-TaskGrid+

Erweiterbares Aufgabenverarbeitungssystem in Containern — Prüfungsaufgabe Verteilte Systeme (TIK23, Dozent: Kevin Dallmann)

**Abgabe:** 08.06.2026 EOB

---

## Systemübersicht

Das System besteht aus 4 Komponenten, die ausschließlich über UDP/RPC kommunizieren (keine shared Volumes für Task-Transfer):

| Komponente | Aufgabe |
|---|---|
| **Client** | Sendet Tasks, fragt Ergebnisse ab |
| **Dispatcher** | Nimmt Tasks entgegen, verwaltet Queue & Zustandsmaschine, leitet an Worker weiter |
| **Namensdienst** | Service Discovery — selbst implementiert (kein Docker-DNS-Ersatz!) |
| **Worker** | Registriert sich, empfängt Tasks, verarbeitet und sendet Ergebnis zurück |

---

## Team & Workload-Aufteilung

### Gemeinsam (alle 3)
Issues #2, #3, #4, #5, #6, #7, #8, #57 — Entscheidungsthemen

Technologiestack, Systemarchitektur, Zustandsmodelle, Nachrichtenformat, ADRs, finale Abgabe.

---

### soniaasni — Dispatcher + Docker
**16 exklusive Issues · 3 PFLICHT · 24 gesamt**

| Bereich | Issues |
|---|---|
| Dispatcher (POST_TASK, Worker-Lookup, Zustandsverwaltung, RESULT_RETURN, GET_RESULT, Timeout/Retry, Logs, GET_STATUS, Queue) | #13–#21 |
| Docker (alle Dockerfiles + docker-compose.yml + Skalierung) | #34–#39 |
| Sonstiges | #1 |

> Der Dispatcher ist die architektonisch komplexeste Komponente: Zustandsmaschine (8 Zustände), Task-Queue, Concurrency, dynamischer Worker-Lookup über Namensdienst (kein statisch!), Retry-Logik.

---

### Xplosy — Namensdienst + Fehlerfälle + Tests
**16 exklusive Issues · 5 PFLICHT · 24 gesamt**

| Bereich | Issues |
|---|---|
| Namensdienst (REGISTER_WORKER, LOOKUP_WORKER, HEARTBEAT + Inaktivitätserkennung, DEREGISTER_WORKER) | #9–#12 |
| Fehlerfälle (7 Szenarien: unbekannter Tasktyp, kein Worker, Worker-Ausfall, Timeout, Doppelergebnis, Namensdienst nicht erreichbar, ungültige Payload) | #40–#46 |
| Testprotokoll (6 erfolgreiche Tests + 3 Fehlerfälle + E2E Smoke-Test) | #47–#49 |
| Doku (Zustandsmodell, Namensdienst & Heartbeat) | #52, #53 |

> Namensdienst ist ein vollständig eigenständiger Service — selbst implementiert, kein Docker-DNS. Fehlerfälle und Tests sind direkt an diesen Bereich gekoppelt.

---

### Elena7x — Worker + Client + Tasktypen + Dokumentation
**17 exklusive Issues · 8 PFLICHT · 25 gesamt**

| Bereich | Issues |
|---|---|
| Worker (Registrierung, Heartbeat-Sender, Task-Verarbeitung, Fehlerbehandlung, Shutdown) | #22–#26 |
| Client (send_task, request_result) | #27–#28 |
| Tasktypen: `reverse`, `sum`, `hash` (PFLICHT) + `upper`, `wait` | #29–#33 |
| Doku (Architekturdiagramm, Kommunikationsprotokoll, ADRs, Testprotokoll-PDF, Startanleitung) | #50, #51, #54–#56 |

> 8 PFLICHT-Issues klingen viel — die Tasktypen (reverse, sum, hash) sind jedoch je 3–10 Zeilen Implementierung. Die Dokumentation ist dafür der umfangreichste Einzelblock im Projekt.

---

## Workload-Vergleich

```
              | Exklusiv | PFLICHT | Gesamt
  soniaasni   |    16    |    3    |   24
  Xplosy      |    16    |    5    |   24
  Elena7x     |    17    |    8    |   25
```

**Geschätzte Stunden:**
- soniaasni: Dispatcher (~8h) + Docker (~4h) + Shared (~3h) ≈ **15h**
- Xplosy: Namensdienst (~5h) + Fehlerfälle (~4h) + Tests (~3h) + Doku (~2h) + Shared (~3h) ≈ **17h**
- Elena7x: Worker (~4h) + Client (~2h) + Tasktypen (~2h) + Doku (~4h) + Shared (~3h) ≈ **15h**

---

## Nachrichtentypen

| Nachricht | Richtung | Beschreibung |
|---|---|---|
| `POST_TASK` | Client → Dispatcher | Task einreichen |
| `GET_RESULT` | Client → Dispatcher | Ergebnis abholen |
| `RESULT_RETURN` | Worker → Dispatcher | Ergebnis liefern |
| `REGISTER_WORKER` | Worker → Namensdienst | Registrierung |
| `HEARTBEAT` | Worker → Namensdienst | Lebenszeichen |
| `LOOKUP_WORKER` | Dispatcher → Namensdienst | Worker für Tasktyp finden |
| `DEREGISTER_WORKER` | Worker → Namensdienst | Abmeldung |
| `GET_STATUS` | Client → Dispatcher | Systemstatus abfragen |

---

## Bewertungskriterien (100 Punkte)

| Kriterium | Punkte |
|---|---|
| Basisfunktionen | 10 |
| Namensdienst | 10 |
| Mehrere Worker | 10 |
| Kommunikation & Logging | 15 |
| Queue & Zustandsmodell | 15 |
| Robustheit | 15 |
| Erweiterbarkeit | 10 |
| Docker | 5 |
| Dokumentation | 10 |
| Tests | 10 |

**Mindestanforderung zum Bestehen:** Alle mit PFLICHT markierten Issues müssen vollständig implementiert sein.

---

## Projektplan

Detaillierte Planung mit Phasen, Meilensteinen und Checkliste: [PLAN.md](PLAN.md)
