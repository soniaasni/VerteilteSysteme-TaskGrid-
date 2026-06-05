# ──────────────────────────────────────────────────────────────────────────────
# TaskGrid+ — Namensdienst Dockerfile
# Issue #37 (soniaasni)
#
# Build-Kontext: Projekt-Root (context: .)
#   docker-compose.yml: build: { context: ., dockerfile: src/namensdienst/Dockerfile }
#
# Der Namensdienst braucht aus dem Root:
#   src/namensdienst/requirements.txt — grpcio Runtime-Abhängigkeit (>=1.80)
#   proto/                             — generierte gRPC-Stubs (shared)
#   src/namensdienst/                  — Service-Discovery-Implementierung
#   src/common/                        — Logger (§13) und Protocol-Typen
#
# Verhalten (gemäß Aufgabenstellung):
#   - Worker-Registrierungen werden ausschließlich im Arbeitsspeicher gehalten
#   - Bei Container-Neustart gehen alle Registrierungen verloren (§4.1)
#   - Worker müssen sich nach jedem Neustart erneut registrieren (§4.3)
#   - Inaktive Worker werden per Heartbeat-Timeout erkannt und entfernt (§4.4)
# ──────────────────────────────────────────────────────────────────────────────

FROM python:3.11-slim

LABEL maintainer="soniaasni"
LABEL component="namensdienst"
LABEL description="TaskGrid+ Namensdienst — Service Discovery, Worker-Registrierung, Heartbeat-Überwachung"

WORKDIR /app

# ── Abhängigkeiten installieren ───────────────────────────────────────────────
# requirements.txt zuerst → Docker-Layer-Cache:
# grpcio-Installation wird nur wiederholt wenn requirements.txt geändert wurde.
COPY src/namensdienst/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Quelltext kopieren ────────────────────────────────────────────────────────
# Proto-Stubs (shared zwischen allen Komponenten)
COPY proto/ ./proto/

# Namensdienst-Implementierung + gemeinsame Module
COPY src/namensdienst/ ./src/namensdienst/
COPY src/common/       ./src/common/

# ── Python-Pfad ───────────────────────────────────────────────────────────────
# /app als Root → "from proto import ..." und "from src.namensdienst import ..." funktionieren.
# /app/proto zusätzlich → generierter gRPC-Code verwendet "import taskgrid_pb2"
# (absoluter Import seit grpcio>=1.80), proto/-Verzeichnis muss direkt
# im sys.path liegen damit dieser Import aufgelöst werden kann.
ENV PYTHONPATH=/app:/app/proto

# ── Log-Verzeichnis ───────────────────────────────────────────────────────────
# Strukturierte Logs gemäß §13; per Volume in docker-compose.yml montierbar.
ENV LOG_DIR=/app/logs
RUN mkdir -p /app/logs

# ── Konfiguration (Umgebungsvariablen, alle via docker-compose überschreibbar) ─
#
# NAMESERVICE_PORT               : gRPC-Port des Namensdiensts (server.py)
# HEARTBEAT_TIMEOUT_SECONDS      : Zeitspanne ohne Heartbeat, nach der ein Worker
#                                  als inaktiv gilt und aus der Registry entfernt wird
# HEARTBEAT_CHECK_INTERVAL_SECONDS: Wie oft der Namensdienst die Registry auf
#                                  abgelaufene Heartbeats prüft
#
# KEIN persistenter Speicher (§4.1): Worker-Registrierungen leben nur im RAM.
# Nach Container-Neustart ist die Registry leer — Worker re-registrieren sich
# automatisch über ihren eigenen Startup (§4.3).

ENV NAMESERVICE_PORT=50052
ENV HEARTBEAT_TIMEOUT_SECONDS=15
ENV HEARTBEAT_CHECK_INTERVAL_SECONDS=5

# ── Port exponieren ───────────────────────────────────────────────────────────
# EXPOSE dokumentiert den gRPC-Port; tatsächliches Mapping erfolgt in
# docker-compose.yml (ports: ["50052:50052"]).
EXPOSE 50052

# ── Start ─────────────────────────────────────────────────────────────────────
CMD ["python", "src/namensdienst/server.py"]
