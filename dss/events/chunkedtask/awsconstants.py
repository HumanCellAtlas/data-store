import os

TASK_SNS_TOPIC_PREFIX = "dss-chunked-task-"
TASK_RETRY_QUEUE_PREFIX = "dss-chunked-task-"

CLIENT_KEY = "chunked_worker_client"
REQUEST_VERSION_KEY = "request_version"
TASK_ID_KEY = "chunked_worker_task_id"
STATE_KEY = "state"

LOG_GROUP_NAME = "chunked_task_worker"
FALLBACK_LOG_STREAM_NAME = "fallback"

CURRENT_VERSION = 1
MIN_SUPPORTED_VERSION = 1
MAX_SUPPORTED_VERSION = 1


# This should be an enum, but doing so makes it cumbersome to serialize to json.
class LogActions:
    SCHEDULED = "scheduled"
    RESCHEDULED = "rescheduled"
    RUNNING = "running"
    EXCEPTION = "exception"
    COMPLETE = "complete"


def get_worker_sns_topic():
    deployment_stage = os.getenv("DSS_DEPLOYMENT_STAGE")
    return TASK_SNS_TOPIC_PREFIX + deployment_stage
