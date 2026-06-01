import os
import sys
import uuid
import time
import grpc

from proto import taskgrid_pb2
from proto import taskgrid_pb2_grpc


DISPATCHER_ADDRESS = os.getenv("DISPATCHER_ADDRESS", "dispatcher:50052")
CLIENT_ID = os.getenv("CLIENT_ID", "client-1")


def send_task(task_type: str, payload: str):
    request_id = str(uuid.uuid4())

    try:
        with grpc.insecure_channel(DISPATCHER_ADDRESS) as channel:
            stub = taskgrid_pb2_grpc.DispatcherServiceStub(channel)

            response = stub.PostTask(
                taskgrid_pb2.PostTaskRequest(
                    message_type="POST_TASK",
                    request_id=request_id,
                    timestamp=int(time.time()),
                    sender=CLIENT_ID,
                    task_type=task_type,
                    task_payload=payload,
                )
            )

        if response.success:
            print(f"Task wurde angenommen. Task-ID: {response.task_id}")
            return response.task_id

        print(f"Fehler: {response.message}")
        return None

    except grpc.RpcError as error:
        print(
            "Verbindungsfehler zum Dispatcher: "
            f"{error.details() or error.code()}"
        )
        return None

    except Exception as error:
        print(f"Unerwarteter Client-Fehler: {error}")
        return None


def main():
    if len(sys.argv) < 3:
        print("Nutzung:")
        print("python client.py <task_type> <payload>")
        print('Beispiel: python client.py reverse "Hallo"')
        return

    task_type = sys.argv[1]
    payload = " ".join(sys.argv[2:])

    send_task(task_type, payload)


if __name__ == "__main__":
    main()