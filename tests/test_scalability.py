import os
import sys
import uuid

from locust import Locust, TaskSet, task


sys.path.append(os.getcwd())
from tests.scalability.SnsClient import SnsLocust
from tests.scalability.TaskSetStart import TaskSetStart

FILE_COUNT = 2
LARGE_FILE_COUNT = 2
test_file_keys = []
test_large_file_keys = []

taskSetInit = TaskSetStart()

class MyTaskSet(TaskSet):
    def on_start(self):
        if not taskSetInit.isStarted():
            print(">>>>>>>>>>>> Started")
            for _ in range(FILE_COUNT):
                test_file_keys.append(f"dss-scalability-test/{uuid.uuid4()}")

            for _ in range(LARGE_FILE_COUNT):
                test_large_file_keys.append(f"dss-scalability-test/large/{uuid.uuid4()}")

            sns_msg = dict(
                test_file_keys=test_file_keys,
                test_large_file_keys=test_large_file_keys
            )

            sns_topic = "dss-scalability-init"
            self.client.invoke(sns_topic, sns_msg)
            taskSetInit.start()

    def put_file(self):
        sns_msg = dict(
            test_file_keys=test_file_keys,
            test_large_file_keys=test_large_file_keys
        )
        sns_topic = "dss-scalability-put-file"
        self.client.invoke(sns_topic, sns_msg)

    @task(1)
    def checkout(self):
        sns_msg = dict(
            test_file_keys=test_file_keys,
            test_large_file_keys=test_large_file_keys
        )
        sns_topic = "dss-scalability-checkout"
        #self.invoke_test(sns_topic, sns_msg)


class MyLocust(SnsLocust):
    task_set = MyTaskSet
    min_wait = 500
    max_wait = 3000

