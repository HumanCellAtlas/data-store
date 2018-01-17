import json
import os
import time

import sys

import boto3
from locust import events, Locust

#from dss.util.aws import send_sns_msg, ARN

sns = boto3.client('sns')

class SnsClient():
    def invoke(self, topic, msg):
        stage = os.environ["DSS_DEPLOYMENT_STAGE"]
        region = "us-east-1"
        account_id = "861229788715"
        sns_topic_arn = f"arn:aws:sns:{region}:{account_id}:{topic}-{stage}"
        start_time = time.time()

        sys.path.append("/Users/romankisin/hca/data-store")

        try:
            self.publish(sns_topic_arn, msg)
        except Exception as e:
            print(f"Failed to post to SNS topic {sns_topic_arn}: {str(e)}")
            total_time = int((time.time() - start_time) * 1000)
            events.request_failure.fire(request_type="sns", name=sns_topic_arn, response_time=total_time, exception=e)
        else:
            total_time = int((time.time() - start_time) * 1000)
            events.request_success.fire(request_type="sns", name=sns_topic_arn, response_time=total_time, response_length=0)

    def publish(self, sns_topic_arn, msg):
        publish_args = {
            'Message': json.dumps(msg),
            'TopicArn': sns_topic_arn
        }
        sns.publish(**publish_args)


class SnsLocust(Locust):
     def __init__(self, *args, **kwargs):
        super(SnsLocust, self).__init__(*args, **kwargs)
        self.client = SnsClient()