# ──────────────────────────────────────────────────────────────────────────────
# TaskGrid+ — Worker Dockerfile
# Issue #36 (soniaasni)
#
# Build-Kontext: ./worker
#   docker-compose.yml: build: { context: ./worker }
#
# Der Worker ist vollständig eigenständig — alle benötigten Dateien liegen
# im worker/-Verzeichnis:
#   requirements.txt         — grpcio Runtime-Abhängigkeit (>=1.80)
#   generated/               — gRPC-Stubs (lokal generiert, nicht shared)
#   task_handlers/           — Handler-Module pro Aufgabentyp
#   worker.py                — Einstiegspunkt
#
# PARAMETRISIERUNG (Issue-Anforderung):
#   Der Worker-Typ wird ausschließlich per Umgebungsvariable konfiguriert.
#   Eine Instanz kann einen oder mehrere Typen unterstützen (WORKER_TYPE
#   als kommagetrennte Liste).
#   Bei docker compose up --scale worker=N erhält jede Instanz eine
#   eindeutige WORKER_ID (worker-<WORKER_TYPE>-<hostname>) via entrypoint.sh.
#
# ENV-VAR-MAPPING (entrypoint.sh):
#   Issue-spezifische Namen  →  worker.py-interne Namen
#   WORKER_TYPE              →  TASK_TYPES
#   NAMENSDIENST_ADDRESS     →  NAMING_SERVICE_ADDRESS
#   HEARTBEAT_INTERVAL_SECONDS → HEARTBEAT_INTERVAL
# ──────────────────────────────────────────────────────────────────────────────

FROM python:3.11-slim

LABEL maintainer="soniaasni"
LABEL component="worker"
LABEL description="TaskGrid+ Worker — Aufgabenverarbeitung, parametrisierbar per Tasktyp"

WORKDIR /app

# ── Abhängigkeiten installieren ───────────────────────────────────────────────
# requirements.txt zuerst → Docker-Layer-Cache:
# grpcio-Installation wird nur wiederholt wenn requirements.txt geändert wurde.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Quelltext kopieren ────────────────────────────────────────────────────────
# Generierte gRPC-Stubs (lokal im worker/-Verzeichnis)
COPY generated/ ./generated/

# Handler-Module (je ein Modul pro Aufgabentyp)
COPY task_handlers/ ./task_handlers/

# Worker-Einstiegspunkt
COPY worker.py .

# Entrypoint-Skript: ENV-VAR-Mapping + eindeutige WORKER_ID bei Skalierung
COPY entrypoint.sh .
# sed entfernt Windows-Zeilenenden (CR), falls das Repo auf Windows ausgecheckt
# wurde — CRLF bricht den Shebang und macht das Skript im Linux-Container unbrauchbar.
RUN sed -i 's/\r//' entrypoint.sh && chmod +x entrypoint.sh

# ── Python-Pfad ───────────────────────────────────────────────────────────────
# /app als Root → "from generated import ..." funktioniert ohne Pfad-Manipulation.
ENV PYTHONPATH=/app

# ── Log-Verzeichnis ───────────────────────────────────────────────────────────
ENV LOG_DIR=/app/logs
RUN mkdir -p /app/logs

# ── Konfiguration (Umgebungsvariablen, alle via docker-compose überschreibbar) ─
#
# WORKER_TYPE          : Kommagetrennte Aufgabentypen dieser Instanz
#                        (z. B. "reverse" oder "reverse,sum,hash,upper,wait")
#                        Wird in TASK_TYPES für worker.py übersetzt.
# WORKER_HOST          : Docker-interner Service-Name (DNS-Auflösung durch Docker)
#                        Dispatcher verwendet diesen Namen für ExecuteTask-Aufruf.
# WORKER_PORT          : gRPC-Port des Workers (nur im Container)
# NAMENSDIENST_ADDRESS : Vollständige gRPC-Adresse des Namensdiensts
#                        Wird in NAMING_SERVICE_ADDRESS für worker.py übersetzt.
# DISPATCHER_ADDRESS   : Vollständige gRPC-Adresse des Dispatchers
#                        (für RESULT_RETURN nach Aufgabenverarbeitung)
# HEARTBEAT_INTERVAL_SECONDS : Heartbeat-Intervall in Sekunden
#                        Wird in HEARTBEAT_INTERVAL für worker.py übersetzt.
#
# WORKER_ID wird NICHT statisch gesetzt — entrypoint.sh generiert eine
# eindeutige ID pro Container-Instanz (worker-<WORKER_TYPE>-<hostname>).
# Das ermöglicht docker compose up --scale worker=N ohne Namenskonflikte.

ENV WORKER_TYPE=reverse,sum,hash,upper,wait
ENV WORKER_HOST=worker
ENV WORKER_PORT=50051
ENV NAMENSDIENST_ADDRESS=namensdienst:50052
ENV DISPATCHER_ADDRESS=dispatcher:50051
ENV HEARTBEAT_INTERVAL_SECONDS=5

# ── Port exponieren ───────────────────────────────────────────────────────────
# EXPOSE dokumentiert den Worker-gRPC-Port; tatsächliches Mapping
# entfällt bei --scale (kein statisches Port-Mapping möglich).
EXPOSE 50051

# ── Start ─────────────────────────────────────────────────────────────────────
# entrypoint.sh mappt ENV-Vars und übergibt an worker.py via exec
ENTRYPOINT ["./entrypoint.sh"]
