import os
import sys
import uuid

from locust import TaskSet, task


sys.path.append(os.getcwd())

from tests.scalability.SfnClient import SfnLocust

test_run_id = str(uuid.uuid4())

class MyTaskSet(TaskSet):
    def on_start(self):
        pass


    @task
    def check_execution(self):
        self.client.start_test_execution(test_run_id)
        self.client.check_execution()


class MyLocust(SfnLocust):
    task_set = MyTaskSet
    min_wait = 500
    max_wait = 3000
