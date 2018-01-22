import os
import sys

from locust import TaskSet, task


sys.path.append(os.getcwd())

from tests.scalability.SfnClient import SfnLocust

class MyTaskSet(TaskSet):
    def on_start(self):
        self.client.start_execution()

    @task
    def check_execution(self):
        self.client.check_execution()


class MyLocust(SfnLocust):
    task_set = MyTaskSet
    min_wait = 500
    max_wait = 3000
