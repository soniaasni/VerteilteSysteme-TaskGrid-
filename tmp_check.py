import os
import sys
import time

sys.path.insert(0, '.')
sys.path.insert(0, 'proto')
os.environ['DISPATCHER_ADDRESS'] = 'localhost:50051'

from src.client.client import send_task, request_result


task_id = send_task('sum', '1,2')
print('TASK_ID', task_id)
for i in range(10):
    time.sleep(1)
    res = request_result(task_id)
    print('ITER', i, 'STATUS', res.payload.status if res else None, 'RESULT', res.payload.result if res else None)
    if res and res.payload.status in ('COMPLETED', 'FAILED'):
        break
