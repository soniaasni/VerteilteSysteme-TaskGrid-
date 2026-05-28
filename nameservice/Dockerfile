# ──────────────────────────────────────────────────────────────────────────────
# TaskGrid+ — Namensdienst Dockerfile
# Issue #37 (soniaasni)
#
# Build-Kontext: ./nameservice
#   docker-compose.yml: build: { context: ./nameservice }
#
# Der Namensdienst ist vollständig eigenständig — alle benötigten Dateien
# liegen im nameservice/-Verzeichnis:
#   requirements.txt       — grpcio Runtime-Abhängigkeit (>=1.80)
#   nameservice.py         — Einstiegspunkt (Service Discovery)
#   generated/             — gRPC-Stubs (werden bei Implementierung ergänzt)
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
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Quelltext kopieren ────────────────────────────────────────────────────────
# Namensdienst-Implementierung
COPY nameservice.py .

# Generierte gRPC-Stubs (werden bei vollständiger Implementierung ergänzt)
# COPY generated/ ./generated/

# ── Python-Pfad ───────────────────────────────────────────────────────────────
# /app als Root → "from generated import ..." funktioniert ohne Pfad-Manipulation.
ENV PYTHONPATH=/app

# ── Log-Verzeichnis ───────────────────────────────────────────────────────────
# Strukturierte Logs gemäß §13; per Volume in docker-compose.yml montierbar.
ENV LOG_DIR=/app/logs
RUN mkdir -p /app/logs

# ── Konfiguration (Umgebungsvariablen, alle via docker-compose überschreibbar) ─
#
# NAMENSDIENST_PORT              : gRPC-Port des Namensdiensts
#                                  (docker-compose überschreibt auf 50052)
# HEARTBEAT_TIMEOUT_SECONDS      : Zeitspanne ohne Heartbeat, nach der ein Worker
#                                  als inaktiv gilt und aus der Registry entfernt wird
# HEARTBEAT_CHECK_INTERVAL_SECONDS: Wie oft der Namensdienst die Registry auf
#                                  abgelaufene Heartbeats prüft
#
# KEIN persistenter Speicher (§4.1): Worker-Registrierungen leben nur im RAM.
# Nach Container-Neustart ist die Registry leer — Worker re-registrieren sich
# automatisch über ihren eigenen Startup (§4.3).

ENV NAMENSDIENST_PORT=6000
ENV HEARTBEAT_TIMEOUT_SECONDS=15
ENV HEARTBEAT_CHECK_INTERVAL_SECONDS=5

# ── Port exponieren ───────────────────────────────────────────────────────────
# EXPOSE dokumentiert den gRPC-Port; tatsächliches Mapping erfolgt in
# docker-compose.yml (ports: ["50052:50052"]).
EXPOSE 6000

# ── Start ─────────────────────────────────────────────────────────────────────
CMD ["python", "nameservice.py"]
