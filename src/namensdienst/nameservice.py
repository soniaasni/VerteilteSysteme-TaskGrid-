import time
import grpc
from src.namensdienst.worker import Worker
from proto import taskgrid_pb2, taskgrid_pb2_grpc

class Namensdienst:
    def __init__(self):
        self.workers = []
        self.idcount = 0

    def RegisterWorker(self,type,address,port):
        self.workers.append(Worker(type,address,port,self.idcount,self))
        self.idcount += 1
        print("Registered Worker of type " + type + " with address " + address + ":" + port)
        return taskgrid_pb2.RegisterResponse(

        )

    def LookupWorker(self,type):
        result = []
        for worker in self.workers:
            if worker.type==type and worker.status != "UNHEALTHY" and worker.status != "OFFLINE":
                result.append(worker)
        print("Found workers of type " + type + ": ")
        for worker in result:
            print("Worker " + str(worker.id) + ": "+ worker.address + ", " + worker.port)
        return taskgrid_pb2.LookupResponse(

        )
        

    def DeregisterWorker(self,address,port):
        for worker in self.workers:
            if worker.address == address and worker.port == port:
                self.workers.remove(worker)
                print("De-Registered Worker with address " + address + ":" + port)
                return True
        print("Could not find Worker with address " + address + " and port " + port)
        return taskgrid_pb2.DeregisterResponse(

        )
    
    def SendHeartbeat(self,worker_id,load):
        for worker in self.workers:
            if worker.id == worker_id:
                worker.lastHeartbeat = time.time()
                worker.currentLoad = load
                return True
        return False

    def startLoop():
        i = 1
    def endLoop():
        i = 1

        
testDienst = Namensdienst()
testDienst.register_worker("flame_grill","google.com","98765")
testDienst.register_worker("flame_grill","amazon.com","12345")
testDienst.lookup_worker("flame_grill")
testDienst.deregister_worker("google.com","98765")


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