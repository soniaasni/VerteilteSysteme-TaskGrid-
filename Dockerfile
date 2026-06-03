# ──────────────────────────────────────────────────────────────────────────────
# TaskGrid+ — Dispatcher Dockerfile
# Issue #35 (soniaasni)
#
# Build-Kontext: Projekt-Root (context: .)
#   docker-compose.yml: build: { context: ., dockerfile: Dockerfile }
#
# Der Dispatcher braucht aus dem Root:
#   requirements.txt         — grpcio Runtime-Abhängigkeit
#   proto/                   — generierte gRPC-Stubs (shared mit Client/Worker)
#   src/dispatcher/          — Dispatcher-Quelltext (server, servicer, loop, ...)
#   src/common/              — Logger (§13) und Protocol-Typen
#
# WICHTIG (Issue-Anforderung): Keine Worker-Adressen im Dockerfile oder als
# Standard-Umgebungsvariablen. Der Dispatcher löst Worker ausschließlich
# dynamisch über den Namensdienst auf (§4.2, §18 Aufgabenstellung).
# ──────────────────────────────────────────────────────────────────────────────

FROM python:3.11-slim

LABEL maintainer="soniaasni"
LABEL component="dispatcher"
LABEL description="TaskGrid+ Dispatcher — Aufgabenverwaltung, Zustandsmaschine, Worker-Dispatch"

WORKDIR /app

# ── Abhängigkeiten installieren ───────────────────────────────────────────────
# Zuerst nur requirements.txt → Docker-Layer-Cache:
# grpcio-Installation (~30s) wird nur neu ausgeführt wenn requirements.txt
# geändert wurde, nicht bei jeder Code-Änderung.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Quelltext kopieren ────────────────────────────────────────────────────────
# Proto-Stubs (gRPC-Interface zwischen allen Komponenten)
COPY proto/ ./proto/

# Dispatcher- und gemeinsame Module
COPY src/ ./src/

# ── Python-Pfad ───────────────────────────────────────────────────────────────
# /app als Root → "from proto import ..." und "from src.dispatcher import ..."
# funktionieren ohne relative Pfad-Manipulation.
# /app als Root → "from proto import ..." und "from src.dispatcher import ..."
# /app/proto zusätzlich → generierter gRPC-Code verwendet "import taskgrid_pb2"
# (absoluter Import seit grpcio>=1.80), proto/-Verzeichnis muss direkt im sys.path
# liegen damit dieser Import aufgelöst werden kann.
ENV PYTHONPATH=/app:/app/proto

# ── Log-Verzeichnis ───────────────────────────────────────────────────────────
# Strukturierte Logs gemäß §13; wird per Volume in docker-compose.yml gemountet.
ENV LOG_DIR=/app/logs
RUN mkdir -p /app/logs

# ── Konfiguration (Umgebungsvariablen, alle via docker-compose überschreibbar) ─
#
# DISPATCHER_PORT      : gRPC-Port des Dispatchers          (src/dispatcher/server.py)
# STATUS_HTTP_PORT     : HTTP-Port für GET /status          (src/dispatcher/http_status_server.py)
# NAMENSDIENST_HOST    : Hostname des Namensdiensts         (src/dispatcher/namensdienst_client.py)
# NAMENSDIENST_PORT    : gRPC-Port des Namensdiensts        (src/dispatcher/namensdienst_client.py)
# DISPATCH_TIMEOUT_SECONDS : Timeout pro Task in Sekunden   (src/dispatcher/dispatch_loop.py)
# MAX_RETRIES          : Maximale Retry-Versuche pro Task   (src/dispatcher/dispatch_loop.py)
# DISPATCH_NO_WORKER_RETRY_SECONDS : Wartezeit wenn kein Worker verfügbar
#
# NICHT gesetzt (Aufgabenanforderung): Worker-Adressen oder Worker-Ports.
# Der Dispatcher ermittelt Worker ausschließlich über den Namensdienst (LOOKUP_WORKER).

ENV DISPATCHER_PORT=50051
ENV STATUS_HTTP_PORT=8080
ENV NAMENSDIENST_HOST=namensdienst
ENV NAMENSDIENST_PORT=50052
ENV DISPATCH_TIMEOUT_SECONDS=30
ENV MAX_RETRIES=3
ENV DISPATCH_NO_WORKER_RETRY_SECONDS=2

# ── Ports exponieren ─────────────────────────────────────────────────────────
# EXPOSE dokumentiert die genutzten Ports; tatsächliches Port-Mapping
# erfolgt in docker-compose.yml (ports: ["50051:50051", "8080:8080"]).
EXPOSE 50051
EXPOSE 8080

# ── Start ─────────────────────────────────────────────────────────────────────
CMD ["python", "src/dispatcher/server.py"]
