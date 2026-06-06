import time
import threading
import grpc
from src.namensdienst.worker import Worker
from proto import taskgrid_pb2, taskgrid_pb2_grpc

class Namensdienst:
    def __init__(self):
        self.workers = []
        self.idcount = 0

        self.running = False
        self.loopThread = None

        self.startLoop(self,5,2)

    def RegisterWorker(self,type,address,port):
        self.workers.append(Worker(type,address,port,self.idcount,self))
        self.idcount += 1
        print("Registered Worker of type " + type + " with address " + address + ":" + port)
        return taskgrid_pb2.RegisterResponse(

        )

    def LookupWorker(self,request,context):
        result = []
        for worker in self.workers:
            if worker.type==request.type and worker.status != "UNHEALTHY" and worker.status != "OFFLINE":
                result.append(worker)
        print("Found workers of type " + type + ": ")
        for worker in result:
            print("Worker " + str(worker.id) + ": "+ worker.address + ", " + worker.port)
        return taskgrid_pb2.LookupResponse(

        )
        

    def DeregisterWorker(self,request,context):
        for worker in self.workers:
            if worker.address == request.address and worker.port == request.port:
                self.workers.remove(worker)
                print("De-Registered Worker with address " + request.address + ":" + request.port)
                return True
        print("Could not find Worker with address " + request.address + " and port " + request.port)
        return taskgrid_pb2.DeregisterResponse(

        )
    
    def SendHeartbeat(self,request,context):

        if not request.worker_id_load:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("task_type darf nicht leer sein")
            #log_event(logger, "warning", "POST_TASK_rejected", request_id=rid, reason="missing_task_type")
            return taskgrid_pb2.Ack()

        for worker in self.workers:
            if worker.id == request.worker_id:
                worker.lastHeartbeat = time.time()
                worker.currentLoad = request.load
                worker.status = "ACTIVE"
                return taskgrid_pb2.Ack()

     def startLoop(self, x, y):
        self.running = True

        def loop():
            while self.running:
                now = time.time()

                # Kopie erstellen, damit während des Iterierens gelöscht werden kann
                for worker in self.workers[:]:
                    elapsed = now - worker.lastHeartbeat

                    if elapsed >= x * y:
                        worker.status = "OFFLINE"
                        self.workers.remove(worker)
                        print(f"Worker {worker.id} wurde OFFLINE gesetzt und entfernt")

                    elif elapsed >= x:
                        worker.status = "UNHEALTHY"
                        print(f"Worker {worker.id} wurde UNHEALTHY gesetzt")

                time.sleep(1)

        self.loopThread = threading.Thread(target=loop, daemon=True)
        self.loopThread.start()

    def endLoop(self):
        self.running = False

        if self.loopThread is not None:
            self.loopThread.join()


"""
status
last_heartbeat
current_load

ACTIVE
UNHEALTHY
DRAINING
OFFLINE

AVAILABLE
WORKING
(UNHEALTHY)
OFFLINE

For communication gRPC
"""

"""
TODO:
Antwortinhalte -> Überall nur ack
Nachrichten empfangen? -> Nur noch Heartbeat fehlt
Hintergrundprozess Loop (Worker auf Unhealthy/Offline setzen) ->
Entfernen wenn Offline -> 
Unhealthy/Offline nicht in Suche zurückgeben -> Done



"""