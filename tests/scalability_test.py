import os
import sys
import uuid
import time

from locust import TaskSet, task

sys.path.append(os.getcwd())

from tests.scalability.SnsClient import SnsLocust  # noqa

class MyTaskSet(TaskSet):
    def on_start(self):
        self.test_run_id = str(uuid.uuid4())
        self.test_execution_id = str(uuid.uuid4())

    @task
    def check_execution(self):
        self.client.start_test_execution(self.test_run_id, self.test_execution_id)
        time.sleep(30)
        self.client.check_execution()

class MyLocust(SnsLocust):
    task_set = MyTaskSet
    min_wait = 500
    max_wait = 3000
