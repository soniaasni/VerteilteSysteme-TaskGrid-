# ──────────────────────────────────────────────────────────────────────────────
# TaskGrid+ — Client Dockerfile
# Issue #34 (soniaasni)
#
# Build-Kontext: Projekt-Root (context: .)
#   docker-compose.yml: build: { context: ., dockerfile: src/client/Dockerfile }
#
# Der Client braucht aus dem Root:
#   requirements.txt             — grpcio Runtime-Abhängigkeit
#   proto/                       — generierte gRPC-Stubs (shared mit Dispatcher)
#   src/client/                  — Client-Implementierung
#
# Betriebsmodi:
#   send    — sendet einen Task (task_type + payload), gibt task_id aus
#   result  — fragt Ergebnis für task_id ab
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

# Client-Quelltext
COPY src/client/ ./src/client/

# ── Python-Pfad ───────────────────────────────────────────────────────────────
# /app als Root → "from proto import ..." und "from src.client import ..." funktionieren.
# /app/proto zusätzlich → generierter gRPC-Code verwendet "import taskgrid_pb2"
# (absoluter Import seit grpcio>=1.80), proto/-Verzeichnis muss direkt im sys.path
# liegen damit dieser Import aufgelöst werden kann.
ENV PYTHONPATH=/app:/app/proto

# ── Log-Verzeichnis ───────────────────────────────────────────────────────────
# Strukturierte Logs gemäß §13; Volume-Mount in docker-compose.yml
ENV LOG_DIR=/app/logs
RUN mkdir -p /app/logs

# ── Konfiguration (Umgebungsvariablen, alle überschreibbar) ───────────────────
#
# DISPATCHER_ADDRESS   : gRPC-Adresse des Dispatchers (Service-Name aus docker-compose)
# CLIENT_ID            : Eindeutiger Name dieses Client-Containers (für Logs)

ENV DISPATCHER_ADDRESS=dispatcher:50051
ENV CLIENT_ID=client-1

# ── Start ─────────────────────────────────────────────────────────────────────
CMD ["python", "src/client/client.py"]
