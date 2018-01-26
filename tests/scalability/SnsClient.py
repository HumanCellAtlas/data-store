import json
import os
import time

import sys

import boto3
from locust import events, Locust

#from dss.util.aws import send_sns_msg, ARN
from dss.util.aws import ARN

sns = boto3.client('sns')
sns.meta.config.max_pool_connections = 100
sns_topic = "dss-scalability-test-launch"

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('scalability_test')

client_type = 'sns'

class SnsClient():

    def start_test_execution(self, run_id: str, execution_id: str):
        self.run_id = run_id
        self.execution_id = execution_id
        stage = os.environ["DSS_DEPLOYMENT_STAGE"]
        region = ARN.get_region()
        account_id = ARN.get_account_id()
        sns_topic_arn = f"arn:aws:sns:{region}:{account_id}:{sns_topic}-{stage}"
        start_time = time.time()
        try:
            self.publish(sns_topic_arn)
        except Exception as e:
            print(f"Failed to post to SNS topic {sns_topic_arn}: {str(e)}")
            total_time = int((time.time() - start_time) * 1000)
            events.request_failure.fire(request_type=client_type, name=sns_topic_arn, response_time=total_time, exception=e)

    def publish(self, sns_topic_arn):
        msg = {"run_id": self.run_id, "execution_id": self.execution_id}

        publish_args = {
            'Message': json.dumps(msg),
            'TopicArn': sns_topic_arn
        }
        sns.publish(**publish_args)

    def check_execution(self):
        assert self.execution_id is not None
        time.sleep(30)
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
                    total_time = item["duration"]
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