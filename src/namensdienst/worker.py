import time

class Worker:
    def __init__(self,type,address,port,id,dienst):
        self.type = type
        self.address = address
        self.port = port
        self.id = id
        self.dienst = dienst

        self.lastHeartbeat = time.time()
        self.status = "AVAILABLE"
        self.currentLoad = 0