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
                    payload=taskgrid_pb2.PostTaskRequest.Payload(
                        task_type=task_type,
                        task_payload=payload,
                    )
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

def request_result(task_id: int):
    request_id = str(uuid.uuid4())

    try:
        with grpc.insecure_channel(DISPATCHER_ADDRESS) as channel:
            stub = taskgrid_pb2_grpc.DispatcherServiceStub(channel)

            response = stub.GetResult(
                taskgrid_pb2.GetResultRequest(
                    message_type="GET_RESULT",
                    request_id=request_id,
                    timestamp=int(time.time()),
                    sender=CLIENT_ID,
                    payload=taskgrid_pb2.GetResultRequest.Payload(
                        task_id=task_id,
                    )
                )
            )

        print(f"Task-ID: {task_id}")
        print(f"Status: {response.status}")

        if not response.found:
            print("Task-ID nicht gefunden")
            return response

        if response.status == "COMPLETED":
            print(f"Ergebnis: {response.result}")

        elif response.status == "FAILED":
            print(f"Fehler: {response.error}")

        elif response.status in ["CREATED", "QUEUED", "DISPATCHED", "PROCESSING", "RETRYING"]:
            print(f"Task wird noch verarbeitet (Status: {response.status})")

        elif response.status == "TIMEOUT":
            print(f"Task ist in einen Timeout gelaufen: {response.error}")

        else:
            print(f"Unbekannter Status: {response.status}")

        return response

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
    if len(sys.argv) < 2:
        print("Nutzung:")
        print('Task senden:    python -m src.client.client send <task_type> <payload>')
        print('Ergebnis holen: python -m src.client.client result <task_id>')
        return

    command = sys.argv[1]

    if command == "send":
        if len(sys.argv) < 4:
            print('Fehler: Für "send" brauchst du <task_type> und <payload>')
            print('Beispiel: python -m src.client.client send reverse "Hallo"')
            return

        task_type = sys.argv[2]
        payload = " ".join(sys.argv[3:])

        send_task(task_type, payload)

    elif command == "result":
        if len(sys.argv) < 3:
            print('Fehler: Für "result" brauchst du eine <task_id>')
            print("Beispiel: python -m src.client.client result 42")
            return

        try:
            task_id = int(sys.argv[2])
        except ValueError:
            print("Fehler: task_id muss eine Zahl sein")
            return

        request_result(task_id)

    else:
        print(f"Unbekannter Befehl: {command}")
        print("Erlaubt sind: send, result")

if __name__ == "__main__":
    main()