class Namensdienst:
    def __init__(self,other_args):
        self.other_args = other_args

    register_worker(const char* type, const char* address)
lookup_worker(const char* type)
deregister_worker(const char* address)
receive_heartbeat(const char* worker_id)

class Worker:
    def __init__(self,type,address,port,dienst):
        self.type = type
        self.address = address
        self.port = port
        self.dienst = dienst
    
    def getID(self):
        self.worker_id = self.dienst.get


status
last_heartbeat
current_load

ACTIVE
UNHEALTHY
DRAINING
OFFLINE