import os

TASK_SNS_TOPIC_PREFIX = "dss-chunked-task-"
TASK_RETRY_QUEUE_PREFIX = "dss-chunked-task-"

CLIENT_KEY = "chunked_worker_client"
STATE_KEY = "payload"


def get_worker_sns_topic():
    deployment_stage = os.getenv("DSS_DEPLOYMENT_STAGE")
    return TASK_SNS_TOPIC_PREFIX + deployment_stage
