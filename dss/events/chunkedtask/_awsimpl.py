import itertools
import json
import logging
import typing

import watchtower

from . import awsconstants
from ...util.aws import ARN, send_sns_msg
from .base import Task, Runtime
from .constants import TIME_OVERHEAD_FACTOR


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


# The rest of this file is for unit tests.  The reason they are in this file is because they need to be deployed to the
# lambda.  For production code, it may make sense to disable this, although it's pretty harmless.


AWS_FAST_TEST_CLIENT_NAME = "fasttest"
AWS_FAST_TEST_EST_TIME_MS = 100


class AWSFastTestRuntime(AWSRuntime):
    """
    This is a modified variant of `AWSRuntime` that fakes less time being available than the system would otherwise
    suggest.  The reason for this is to test the serialization and deserialization of state and the scheduling of work
    without taking up the time fo a full lambda timeslice.  Furthermore, this reduces the risk that the test will
    spuriously break at some future point in time if lambda lifetimes become longer.

    This should only be used for the fast test.
    """
    def __init__(self, context, task_id: str) -> None:
        super().__init__(context, AWS_FAST_TEST_CLIENT_NAME, task_id)
        self.time_remaining_iterator = itertools.chain(
            [int(AWS_FAST_TEST_EST_TIME_MS * TIME_OVERHEAD_FACTOR) + 1],
            itertools.repeat(0)
        )

    def get_remaining_time_in_millis(self) -> int:
        return self.time_remaining_iterator.__next__()


class AWSFastTestTask(Task[typing.MutableSequence]):
    """
    This is a chunked task that counts from a number to another.  Once the counting is complete, it prints something to
    console, which is detected by the unit test.
    """
    def __init__(self, state: typing.MutableSequence) -> None:
        self.state = state

    def get_state(self) -> typing.MutableSequence:
        return self.state

    @property
    def expected_max_one_unit_runtime_millis(self) -> int:
        return AWS_FAST_TEST_EST_TIME_MS

    def run_one_unit(self) -> bool:
        if self.state[0] >= self.state[1]:
            return False
        else:
            self.state[0] += 1
            return True  # more work to be done.
