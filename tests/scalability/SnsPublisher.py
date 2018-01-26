import json
import queue
import threading
from queue import Queue

import boto3
import os

import time

from dss.util.aws import ARN

SENDING_THREADS = 3
sns = boto3.client('sns')
# sns.meta.config.max_pool_connections = 100
sns_topic = "dss-scalability-test-launch"
sending_queue = Queue()
stage = os.environ["DSS_DEPLOYMENT_STAGE"]
region = ARN.get_region()
account_id = ARN.get_account_id()
sns_topic_arn = f"arn:aws:sns:{region}:{account_id}:{sns_topic}-{stage}"

def enqueue_message(msg: dict):
    sending_queue.put(msg)

def _publish_messages():
    while True:
        try:
            msg = sending_queue.get(block = True, timeout = 0.25)
        except queue.Empty:
            pass
        else:
            publish_args = {
                'Message': json.dumps(msg),
                'TopicArn': sns_topic_arn
            }
            sns.publish(**publish_args)
            sending_queue.task_done()

threads = []
for i in range(SENDING_THREADS):
    t = threading.Thread(
        target=_publish_messages,
        name='sns-publisher-worker-{}'.format(i))
    threads.append(t)
    t.start()