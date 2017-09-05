import json
import logging
import typing

from . import awsconstants
from ...util.aws import ARN, send_sns_msg
from ...util.aws.logging import log_message
from .base import Runtime


class AWSRuntime(Runtime[dict, typing.Any]):
    """
    This is an implementation of `Runtime` specialized for AWS Lambda.  Work scheduling is done by posting a message
    containing the serialized state to SNS.
    """
    def __init__(self, context, client_name: str, task_id: str) -> None:
        self.context = context
        self.client_name = client_name
        self.task_id = task_id

    def get_remaining_time_in_millis(self) -> int:
        return self.context.get_remaining_time_in_millis()

    def reschedule_work(self, state: dict):
        payload = {
            awsconstants.CLIENT_KEY: self.client_name,
            awsconstants.REQUEST_VERSION_KEY: awsconstants.CURRENT_VERSION,
            awsconstants.TASK_ID_KEY: self.task_id,
            awsconstants.STATE_KEY: state,
        }

        sns_arn = ARN(self.context.invoked_function_arn, service="sns", resource=awsconstants.get_worker_sns_topic())
        send_sns_msg(sns_arn, payload)

        AWSRuntime.log(
            self.task_id,
            json.dumps(dict(
                action=awsconstants.LogActions.RESCHEDULED,
                payload=payload,
            )),
        )

    def work_complete_callback(self, result: typing.Any):
        AWSRuntime.log(
            self.task_id,
            json.dumps(dict(
                action=awsconstants.LogActions.COMPLETE,
                message=result,
            )),
        )

    @staticmethod
    def log(task_id: str, message: str):
        log_message(awsconstants.LOG_GROUP_NAME, task_id, message)
