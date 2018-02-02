import json
import os
import queue
import threading
from queue import Queue

import boto3

from dss.util.aws import ARN

SENDING_THREADS = 4
sns = boto3.client('sns')
sns.meta.config.max_pool_connections = 100
sns_topic_run = "dss-scalability-test-run"
sns_topic_exec = "dss-scalability-test"
sending_queue = Queue()
stage = os.environ["DSS_DEPLOYMENT_STAGE"]

def get_sns_topic_arn(sns_topic):
    return f"arn:aws:sns:{ARN.get_region()}:{ARN.get_account_id()}:{sns_topic}-{stage}"

class SnsPublisher:
    def enqueue_message(self, msg: dict):
        sending_queue.put(msg)

    def send_start_run(self, run_id: str):
        msg = {'run_id': run_id}
        self._send(sns_topic_run, msg)

    def _send(self, sns_topic, msg):
        publish_args = {
            'Message': json.dumps(msg),
            'TopicArn': get_sns_topic_arn(sns_topic)
        }
        sns.publish(**publish_args)

    def _publish_messages(self):
        while self.started:
            try:
                msg = sending_queue.get(block=True, timeout=0.25)
            except queue.Empty:
                pass
            else:
                self._send(sns_topic_exec, msg)
                sending_queue.task_done()

    def start(self):
        self.started = True
        threads = []
        for i in range(SENDING_THREADS):
            t = threading.Thread(
                target=self._publish_messages,
                name='sns-publisher-worker-{}'.format(i))
            threads.append(t)
            t.start()

    def stop(self):
        self.started = False
