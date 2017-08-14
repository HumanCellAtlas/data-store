import json
import logging
import typing

import watchtower

from . import awsconstants
from ...util.aws import ARN, send_sns_msg
from .base import Runtime


class AWSRuntime(Runtime[dict]):
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

    def schedule_work(self, state: dict):
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

    def work_complete_callback(self):
        AWSRuntime.log(
            self.task_id,
            json.dumps(dict(
                action=awsconstants.LogActions.COMPLETE,
            )),
        )

    logger = dict()  # type: typing.Mapping[str, logging.Logger]

    @staticmethod
    def log(task_id: str, message: str):
        logger_name = f"chunkedtasklogger-{task_id}"

        logger = AWSRuntime.logger.get(logger_name, AWSRuntime._make_logger(logger_name, task_id))
        logger.info(message)

    @staticmethod
    def _make_logger(logger_name: str, task_id: str) -> logging.Logger:
        logger = logging.getLogger(logger_name)
        logger.propagate = False
        handler = watchtower.CloudWatchLogHandler(
            log_group=awsconstants.LOG_GROUP_NAME,
            stream_name=task_id,
            use_queues=False)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

        return logger
