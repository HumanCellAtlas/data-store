import itertools
import typing

from ._awsimpl import AWSRuntime
from .constants import TIME_OVERHEAD_FACTOR
from .base import Task

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
