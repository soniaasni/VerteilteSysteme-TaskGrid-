"""
HTTP-Monitoring-Endpoint für GET_STATUS (Issue #20).
Lauscht auf PORT (default 8080) und liefert JSON unter GET /status.
Läuft als Daemon-Thread neben dem gRPC-Server.
"""

from __future__ import annotations

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from src.common.logger import get_logger
from src.dispatcher.status_collector import StatusCollector

logger = get_logger("dispatcher.status_http")

_DEFAULT_PORT = int(os.environ.get("STATUS_HTTP_PORT", "8080"))


class HttpStatusServer(threading.Thread):

    def __init__(self, collector: StatusCollector, port: int = _DEFAULT_PORT) -> None:
        super().__init__(daemon=True, name="status-http")
        self._collector = collector
        self._port      = port
        self._server: HTTPServer | None = None

    def run(self) -> None:
        collector = self._collector

        class _Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path in ("/status", "/status/", "/"):
                    payload = json.dumps(
                        collector.get_status(),
                        ensure_ascii=False,
                        indent=2,
                    ).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                else:
                    self.send_response(404)
                    self.end_headers()

            def log_message(self, fmt, *args):
                logger.debug(fmt % args)

        self._server = HTTPServer(("0.0.0.0", self._port), _Handler)
        logger.info(f"Status-HTTP-Server gestartet auf Port {self._port} → GET /status")
        self._server.serve_forever()

    def stop(self) -> None:
        if self._server:
            self._server.shutdown()
