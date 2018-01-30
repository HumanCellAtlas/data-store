import os
import sys
import uuid
import time

from locust import TaskSet, task

sys.path.append(os.getcwd())

from tests.scalability.SnsClient import SnsLocust, SnsClient  # noqa

test_run_id = str(uuid.uuid4())
print('>>>Run once')
sns_client = SnsClient()
sns_client.start_test_run(test_run_id)

class MyTaskSet(TaskSet):
    def on_start(self):
        self.test_execution_id = str(uuid.uuid4())

    @task
    def check_execution(self):
        self.client.start_test_execution(test_run_id, self.test_execution_id)

class MyLocust(SnsLocust):
    task_set = MyTaskSet
    min_wait = 500
    max_wait = 3000
