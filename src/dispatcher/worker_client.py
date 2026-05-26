"""
Worker-Client des Dispatchers.
Sendet DispatchTask per gRPC an einen Worker.
Die Adresse wird dynamisch übergeben (kommt vom Namensdienst — nie statisch).
"""

import grpc

from proto import taskgrid_pb2, taskgrid_pb2_grpc
from src.common.logger import get_logger, log_event
from src.dispatcher.task import Task

logger = get_logger("dispatcher.worker_client")

DISPATCH_TIMEOUT_SECS = 5.0


class WorkerClient:

    def dispatch_task(self, address: str, port: int, task: Task) -> bool:
        """
        Sendet DispatchTask an Worker unter address:port.
        Gibt True zurück wenn Worker mit ok=True geantwortet hat.
        Gibt False zurück bei Netzwerkfehler oder ok=False.
        Kanal wird nach jedem Aufruf geschlossen (kein Connection-Pooling nötig).
        """
        target = f"{address}:{port}"
        channel = grpc.insecure_channel(target)
        try:
            stub = taskgrid_pb2_grpc.WorkerServiceStub(channel)
            ack = stub.DispatchTask(
                taskgrid_pb2.DispatchRequest(
                    task_id=task.task_id,
                    task_type=task.task_type,
                    payload=task.payload,
                ),
                timeout=DISPATCH_TIMEOUT_SECS,
            )
            if not ack.ok:
                log_event(logger, "warning", "DISPATCH_TASK_nack",
                          task_id=task.task_id, target=target, reason=ack.message)
            return ack.ok

        except grpc.RpcError as e:
            log_event(logger, "error", "DISPATCH_TASK_rpc_error",
                      task_id=task.task_id, target=target, error=str(e.code()))
            return False
        finally:
            channel.close()
