import json
import os
import time
from queue import Queue

import boto3
from locust import events, Locust

from dss.util.aws import ARN
from tests.scalability.SnsPublisher import enqueue_message, sns_topic

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('scalability_test')

client_type = 'sns'

class SnsClient():

    def start_test_execution(self, run_id: str, execution_id: str):
        self.run_id = run_id
        self.execution_id = execution_id
        self.start_time = time.time()
        msg = {"run_id": self.run_id, "execution_id": self.execution_id}
        enqueue_message(msg)

    def check_execution(self):
        assert self.execution_id is not None
        try:
            for i in range(10):
                time.sleep(10)
                response = table.get_item(
                    Key={
                        'execution_id': self.execution_id
                    }
                )
                item = response.get('Item')

                if item is None:
                    pass
                elif item["status"] == 'SUCCEEDED':
                    total_time = float(item["duration"])
                    events.request_success.fire(request_type=client_type, name=sns_topic, response_time=total_time,
                                                response_length=0)
                    break
                else:
                    total_time = item["duration"]
                    events.request_failure.fire(request_type=client_type, name=sns_topic, response_time=total_time,
                                                exception=None)
                    break
        except Exception as e:
            print(f"Failed to get execution results  {sns_topic}: {str(e)}")
            total_time = int((time.time() - self.start_time) * 1000)
            events.request_failure.fire(request_type=client_type, name=sns_topic, response_time=total_time, exception=e)


class SnsLocust(Locust):
    def __init__(self, *args, **kwargs):
        super(SnsLocust, self).__init__(*args, **kwargs)
        self.client = SnsClient()
