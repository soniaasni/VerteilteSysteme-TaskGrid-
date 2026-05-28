#!/bin/sh
# ──────────────────────────────────────────────────────────────────────────────
# TaskGrid+ Worker — Entrypoint
#
# Zweck: Mappt Issue-#36-Umgebungsvariablen auf die internen Namen in worker.py
# und generiert eine eindeutige WORKER_ID pro Container-Instanz.
#
# ENV-VAR-Mapping:
#   WORKER_TYPE              → TASK_TYPES          (kommagetrennte Typen)
#   NAMENSDIENST_ADDRESS     → NAMING_SERVICE_ADDRESS
#   HEARTBEAT_INTERVAL_SECONDS → HEARTBEAT_INTERVAL
#
# WORKER_ID-Generierung:
#   Format: worker-<WORKER_TYPE>-<hostname>
#   Docker vergibt bei --scale worker=N pro Instanz einen eindeutigen Hostnamen
#   (z. B. "abc12f3"), sodass jeder Worker eine unterschiedliche ID hat.
#   Falls WORKER_ID bereits gesetzt ist, wird sie unverändert übernommen.
# ──────────────────────────────────────────────────────────────────────────────

# TASK_TYPES: Issue-Name WORKER_TYPE hat Vorrang, Fallback auf direktes TASK_TYPES
export TASK_TYPES="${WORKER_TYPE:-${TASK_TYPES:-reverse}}"

# NAMING_SERVICE_ADDRESS: Issue-Name NAMENSDIENST_ADDRESS hat Vorrang
export NAMING_SERVICE_ADDRESS="${NAMENSDIENST_ADDRESS:-${NAMING_SERVICE_ADDRESS:-namensdienst:50052}}"

# HEARTBEAT_INTERVAL: Issue-Name HEARTBEAT_INTERVAL_SECONDS hat Vorrang
export HEARTBEAT_INTERVAL="${HEARTBEAT_INTERVAL_SECONDS:-${HEARTBEAT_INTERVAL:-5}}"

# WORKER_ID: eindeutig pro Instanz (hostname = Docker-Container-ID-Prefix)
# Bleibt erhalten wenn bereits von außen gesetzt (z. B. in Tests)
if [ -z "${WORKER_ID}" ]; then
    export WORKER_ID="worker-${TASK_TYPES}-$(hostname)"
fi

exec python worker.py
