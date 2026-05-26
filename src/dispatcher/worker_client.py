"""
Worker-Client des Dispatchers.
Sendet ExecuteTask per gRPC an einen Worker (Elena-kompatibel).
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
        Sendet ExecuteTask an Worker unter address:port (Elena: ExecuteTask, task_payload).
        Gibt True zurück wenn Worker mit status="accepted" geantwortet hat.
        Gibt False zurück bei Netzwerkfehler oder Ablehnung.
        """
        target = f"{address}:{port}"
        channel = grpc.insecure_channel(target)
        try:
            stub = taskgrid_pb2_grpc.WorkerServiceStub(channel)
            response = stub.ExecuteTask(
                taskgrid_pb2.TaskRequest(
                    request_id=task.task_id,
                    task_id=int(task.task_id),       # int32 — Elena-Kompatibilität
                    task_type=task.task_type,
                    task_payload=task.payload,        # Elena: task_payload statt payload
                ),
                timeout=DISPATCH_TIMEOUT_SECS,
            )
            if response.status != "accepted":
                log_event(logger, "warning", "DISPATCH_TASK_nack",
                          task_id=task.task_id, target=target, reason=response.error)
            return response.status == "accepted"

        except grpc.RpcError as e:
            log_event(logger, "error", "DISPATCH_TASK_rpc_error",
                      task_id=task.task_id, target=target, error=str(e.code()))
            return False
        finally:
            channel.close()
