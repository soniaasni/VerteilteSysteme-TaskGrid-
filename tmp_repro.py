import json
import os
import subprocess
import time
import grpc
from proto import taskgrid_pb2, taskgrid_pb2_grpc

REPO_ROOT='.'
DISPATCHER_ADDRESS=os.environ.get('DISPATCHER_ADDRESS','localhost:50051')


def docker(*args):
    return subprocess.run(['docker','compose',*args], cwd=REPO_ROOT, capture_output=True, text=True)


def wait(service, expected='running', timeout=90):
    deadline=time.time()+timeout
    while time.time()<deadline:
        res=docker('ps','--format','json')
        for raw in res.stdout.splitlines():
            if not raw.strip():
                continue
            try:
                entry=json.loads(raw)
            except Exception:
                continue
            if entry.get('Service')==service:
                state=str(entry.get('State','')).lower()
                health=str(entry.get('Health','')).lower()
                if expected in state or (expected=='running' and 'running' in state) or (health and expected in health):
                    return True
        time.sleep(2)
    return False


def post(task_type,payload,request_id):
    with grpc.insecure_channel(DISPATCHER_ADDRESS) as channel:
        stub=taskgrid_pb2_grpc.DispatcherServiceStub(channel)
        response=stub.PostTask(taskgrid_pb2.PostTaskRequest(message_type='POST_TASK',request_id=request_id,timestamp=int(time.time()),sender='manual',payload=taskgrid_pb2.PostTaskRequest.Payload(task_type=task_type,task_payload=payload)))
    print('POST', request_id, response.payload)
    return int(response.payload.task_id)


def get_result(task_id):
    with grpc.insecure_channel(DISPATCHER_ADDRESS) as channel:
        stub=taskgrid_pb2_grpc.DispatcherServiceStub(channel)
        response=stub.GetResult(taskgrid_pb2.GetResultRequest(message_type='GET_RESULT',request_id=f'result-{task_id}',timestamp=int(time.time()),sender='manual',payload=taskgrid_pb2.GetResultRequest.Payload(task_id=task_id)))
    print('GET', task_id, response.payload)
    return response

print('up', docker('up','-d','--build','--remove-orphans'))
print('wait', wait('namensdienst', timeout=180), wait('dispatcher', timeout=180))
first=post('sum','1,2','pre-pause')
for i in range(20):
    r=get_result(first)
    if r.payload.status=='COMPLETED':
        print('first completed')
        break
    time.sleep(2)

print('pause')
subprocess.run(['docker','pause','namensdienst'], cwd=REPO_ROOT, capture_output=True, text=True)
time.sleep(2)
broken=post('sum','4,5','during-pause')
time.sleep(5)
print('broken status', get_result(broken).payload)

print('unpause')
subprocess.run(['docker','unpause','namensdienst'], cwd=REPO_ROOT, capture_output=True, text=True)
print('wait after', wait('namensdienst', timeout=90))
recovered=post('sum','7,8','after-recovery')
for i in range(20):
    r=get_result(recovered)
    print('recovered loop', i, r.payload)
    if r.payload.status=='COMPLETED':
        break
    time.sleep(2)
print('final', get_result(recovered).payload)
