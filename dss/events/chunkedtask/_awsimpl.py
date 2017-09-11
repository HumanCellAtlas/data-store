import json
import typing

from . import aws, awsconstants
from ...util.aws.logging import log_message
from .base import Runtime, Task


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

    def schedule_work(self, task_class: typing.Type[Task[dict, typing.Any]], state: dict, new_task: bool) -> str:
        if new_task:
            task_id = None
        else:
            task_id = self.task_id

        return aws.schedule_task(task_class, state, task_id)

    def work_complete_callback(self, result: typing.Any):
        AWSRuntime.log(
            self.client_name,
            self.task_id,
            json.dumps(dict(
                action=awsconstants.LogActions.COMPLETE,
                message=result,
            )),
        )

    @staticmethod
    def log(client_key: str, task_id: str, message: str):
        log_message(awsconstants.get_worker_sns_topic(client_key), task_id, message)
