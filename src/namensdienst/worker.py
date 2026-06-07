import time


class Worker:
    def __init__(self, type, address, port, id, dienst, worker_id=None, task_types=None):
        self.type = type
        self.address = address
        self.port = port
        self.id = id
        self.worker_id = worker_id or str(id)
        self.task_types = list(task_types or [type])
        self.dienst = dienst

        self.lastHeartbeat = time.time()
        self.status = "ACTIVE"
        self.currentLoad = 0