import json
import boto3
import os
from concurrent.futures import ThreadPoolExecutor

from dss.util.aws import ARN

sns = boto3.client('sns')
sns.meta.config.max_pool_connections = 100
sns_topic_run = "dss-scalability-test-run"
sns_topic_exec = "dss-scalability-test"
stage = os.environ["DSS_DEPLOYMENT_STAGE"]


class SnsClient():
    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=4)

    def get_sns_topic_arn(self, sns_topic):
        return f"arn:aws:sns:{ARN.get_region()}:{ARN.get_account_id()}:{sns_topic}-{stage}"

    def start_test_run(self, run_id: str):
        self.executor.submit(self.send_start_run(run_id))

    def start_test_execution(self, run_id: str, execution_id: str):
        self.execution_id = execution_id
        self.executor.submit(self.send_start_exec(run_id))

    def _send(self, sns_topic, msg):
        publish_args = {
            'Message': json.dumps(msg),
            'TopicArn': self.get_sns_topic_arn(sns_topic)
        }
        sns.publish(**publish_args)

    def send_start_run(self, run_id: str):
        msg = {'run_id': run_id}
        self._send(sns_topic_run, msg)

    def send_start_exec(self, run_id):
        msg = {"run_id": run_id, "execution_id": self.execution_id}
        self._send(sns_topic_exec, msg)
