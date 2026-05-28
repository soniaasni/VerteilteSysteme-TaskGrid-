# ──────────────────────────────────────────────────────────────────────────────
# TaskGrid+ — Client Dockerfile
# Issue #34 (soniaasni)
#
# Build-Kontext: Projekt-Root (context: .)
#   Warum Root statt ./client?
#   Der Client importiert gemeinsam genutzte Module:
#     proto/taskgrid_pb2.py          (gRPC-Stubs, shared mit Dispatcher)
#     proto/taskgrid_pb2_grpc.py
#     src/common/logger.py           (strukturiertes Logging §13)
#     src/common/protocol.py
#   Diese liegen im Root, nicht im client/-Verzeichnis.
#   Das Kopieren in ein eigenes Unterverzeichnis würde Code-Duplizierung erzeugen.
#
# Betriebsmodi (CLIENT_MODE):
#   demo    — sendet Demo-Tasks aller Pflichttypen, wartet auf Ergebnisse
#   send    — sendet einen Task (TASK_TYPE + TASK_PAYLOAD), gibt task_id aus
#   result  — fragt Ergebnis für TASK_ID ab
# ──────────────────────────────────────────────────────────────────────────────

FROM python:3.11-slim

LABEL maintainer="soniaasni"
LABEL component="client"
LABEL description="TaskGrid+ Client — sendet Tasks und fragt Ergebnisse ab"

WORKDIR /app

# ── Abhängigkeiten installieren ───────────────────────────────────────────────
# Nur requirements.txt zuerst kopieren → Docker-Layer-Cache:
# Abhängigkeiten werden nur neu gebaut wenn requirements.txt geändert wurde.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Quelltext kopieren ────────────────────────────────────────────────────────
# Proto-Stubs (shared zwischen Dispatcher und Client)
COPY proto/ ./proto/

# Gemeinsame Module (Logger §13, Protokoll-Typen)
COPY src/ ./src/

# Client-Quelltext
COPY client/ ./client/

# ── Python-Pfad ───────────────────────────────────────────────────────────────
# /app als Root → "from proto import ..." und "from src.common import ..." funktionieren.
ENV PYTHONPATH=/app

# ── Log-Verzeichnis ───────────────────────────────────────────────────────────
# Strukturierte Logs gemäß §13; Volume-Mount in docker-compose.yml
ENV LOG_DIR=/app/logs
RUN mkdir -p /app/logs

# ── Konfiguration (Umgebungsvariablen, alle überschreibbar) ───────────────────
#
# DISPATCHER_ADDRESS   : gRPC-Adresse des Dispatchers (Service-Name aus docker-compose)
# CLIENT_ID            : Eindeutiger Name dieses Client-Containers (für Logs)
# CLIENT_MODE          : Betriebsmodus: demo | send | result
# TASK_TYPE            : Aufgabentyp für CLIENT_MODE=send  (z. B. "reverse")
# TASK_PAYLOAD         : Nutzdaten    für CLIENT_MODE=send  (z. B. "hello")
# TASK_ID              : Task-ID      für CLIENT_MODE=result
# POLL_INTERVAL_SECONDS: Abfrageintervall beim Warten auf Ergebnis (Default: 1.0)
# POLL_TIMEOUT_SECONDS : Maximale Wartezeit auf Ergebnis            (Default: 60.0)
# CONNECT_RETRIES      : Verbindungsversuche zum Dispatcher         (Default: 15)
# CONNECT_RETRY_DELAY_SECONDS: Wartezeit zwischen Verbindungsversuchen (Default: 2.0)

ENV DISPATCHER_ADDRESS=dispatcher:50051
ENV CLIENT_ID=client-1
ENV CLIENT_MODE=demo
ENV POLL_INTERVAL_SECONDS=1.0
ENV POLL_TIMEOUT_SECONDS=60.0
ENV CONNECT_RETRIES=15
ENV CONNECT_RETRY_DELAY_SECONDS=2.0

# ── Start ─────────────────────────────────────────────────────────────────────
CMD ["python", "client/client.py"]
