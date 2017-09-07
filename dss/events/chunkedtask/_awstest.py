import itertools
import json
import sys
import time
import typing

import boto3

from ._awsimpl import AWSRuntime
from .awsconstants import LogActions, get_worker_sns_topic
from .constants import TIME_OVERHEAD_FACTOR
from .base import Runtime, Task


def is_task_complete(client_name: str, task_id: str):
    """
    Scan AWS Cloudwatch logs to check if a chunked task is complete.
    """
    logs_client = boto3.client('logs')
    response = logs_client.filter_log_events(
        logGroupName=get_worker_sns_topic(client_name),
        logStreamNames=[task_id],
    )

    for event in response['events']:
        try:
            message = json.loads(event['message'])
        except json.JSONDecodeError:
            continue

        if message.get('action') == LogActions.COMPLETE:
            return True

    return False


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


class AWSFastTestTask(Task[typing.MutableSequence, bool]):
    """
    This is a chunked task that counts from a number to another.  Once the counting is complete, it prints something to
    console, which is detected by the unit test.
    """
    def __init__(self, state: typing.MutableSequence, *args, **kwargs) -> None:
        self.state = state

    def get_state(self) -> typing.MutableSequence:
        return self.state

    @property
    def expected_max_one_unit_runtime_millis(self) -> int:
        return AWS_FAST_TEST_EST_TIME_MS

    def run_one_unit(self) -> typing.Optional[bool]:
        if self.state[0] >= self.state[1]:
            return True
        else:
            self.state[0] += 1
            return None  # more work to be done.


class SupervisorTask(Task[dict, bool]):
    SPAWNED_TASK_KEY = "key"
    TIMEOUT_KEY = "timeout"
    DEFAULT_TIMEOUT = 30.0

    def __init__(self, state: dict, runtime: Runtime) -> None:
        self.state = state
        self.runtime = runtime
        self.last_checked = None  # type: typing.Optional[float]

    @property
    def expected_max_one_unit_runtime_millis(self) -> int:
        return sys.maxsize

    def get_state(self) -> dict:
        return self.state

    def run_one_unit(self) -> typing.Optional[bool]:
        if SupervisorTask.SPAWNED_TASK_KEY not in self.state:
            # start the job
            task_id = self.runtime.schedule_work(AWSFastTestTask, [0, 5], True)
            timeout = time.time() + SupervisorTask.DEFAULT_TIMEOUT

            self.state[SupervisorTask.SPAWNED_TASK_KEY] = task_id
            self.state[SupervisorTask.TIMEOUT_KEY] = timeout

            return None

        if time.time() > self.state[SupervisorTask.TIMEOUT_KEY]:
            return False

        if self.check_success_marker():
            return True
        return None

    def check_success_marker(self):
        raise NotImplementedError()


AWS_SUPERVISOR_TEST_CLIENT_NAME = "supervisortest"


class AWSSupervisorTask(SupervisorTask):
    def __init__(self, state: dict, runtime: AWSRuntime) -> None:
        super().__init__(state, runtime)

    def check_success_marker(self):
        # don't pound the filter logs API to a pulp.
        if (self.last_checked is not None and
                time.time() < self.last_checked + 1):
            time.sleep(1)

        if is_task_complete(AWS_FAST_TEST_CLIENT_NAME, self.state[SupervisorTask.SPAWNED_TASK_KEY]):
            return True

        self.last_checked = time.time()
        return None
