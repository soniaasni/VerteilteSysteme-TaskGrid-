"""
conftest.py — pytest-Konfiguration.

Fügt proto/ zum sys.path hinzu, damit die gRPC-generierten Stubs
(taskgrid_pb2_grpc.py) das generierte `import taskgrid_pb2` auflösen können.
Die Stubs sind auto-generierte Dateien die absoluten Import verwenden.
"""

import os
import sys

# proto/ in den Suchpfad eintragen damit "import taskgrid_pb2" in grpc-Stubs klappt
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "proto"))
