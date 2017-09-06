#!/usr/bin/env python
# coding: utf-8

from __future__ import absolute_import, division, print_function, unicode_literals

import itertools
import json
import os
import sys
import time
import typing
import unittest

import boto3

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.events import chunkedtask
from dss.events.chunkedtask import aws, awsconstants
from dss.events.chunkedtask._awstest import AWS_FAST_TEST_CLIENT_NAME


class TestChunkedTaskRuntime(chunkedtask.Runtime[tuple, typing.Tuple[int, int]]):
    def __init__(self, initial_time_millis: int, tick_iterator: typing.Iterator[int]) -> None:
        self.remaining_time = initial_time_millis
        self.tick_iterator = tick_iterator
        self.rescheduled_state = None  # type: typing.Optional[tuple]
        self.complete = False

    def get_remaining_time_in_millis(self) -> int:
        return self.remaining_time

    def reschedule_work(self, state: tuple):
        # it's illegal for there to be no state.
        assert state is not None
        self.rescheduled_state = state

    def advance_time(self):
        self.remaining_time -= self.tick_iterator.__next__()

    def work_complete_callback(self, result: typing.Tuple[int, int]):
        self.complete = True


class TestChunkedTask(chunkedtask.Task[typing.Tuple[int, int, int], typing.Tuple[int, int]]):
    def __init__(
            self,
            state: typing.Tuple[int, int, int],
            runtime: TestChunkedTaskRuntime,
            expected_max_one_unit_runtime_millis: int) -> None:
        self.x0, self.x1, self.rounds_remaining = state
        self.runtime = runtime
        self._expected_max_one_unit_runtime_millis = expected_max_one_unit_runtime_millis

    @property
    def expected_max_one_unit_runtime_millis(self) -> int:
        return self._expected_max_one_unit_runtime_millis

    def get_state(self) -> typing.Any:
        return self.x0, self.x1, self.rounds_remaining

    def run_one_unit(self) -> typing.Optional[typing.Tuple[int, int]]:
        x0new = self.x0 + self.x1
        self.x1 = self.x0
        self.x0 = x0new
        self.runtime.advance_time()

        self.rounds_remaining -= 1

        if self.rounds_remaining == 0:
            return self.x0, self.x1
        return None


class TestChunkedTaskRunner(unittest.TestCase):
    def test_workload_resumes(self):
        initial_state = (1, 1, 25)
        expected_max_one_unit_runtime_millis = 10  # we know exactly how long we'll take.  we're so good at guessing!
        tick_iterator = itertools.repeat(10)
        initial_time_millis = 100

        current_state = initial_state

        serialize_count = 0
        while True:
            env = TestChunkedTaskRuntime(initial_time_millis, tick_iterator)
            task = TestChunkedTask(current_state, env, expected_max_one_unit_runtime_millis)
            runner = chunkedtask.Runner(task, env)

            runner.run()

            if env.complete:
                # we're done!
                final_state = task.get_state()
                self.assertEqual(final_state, (196418, 121393, 0))
                self.assertEqual(serialize_count, 2)
                break
            else:
                current_state = env.rescheduled_state
                serialize_count += 1


class TestAWSChunkedTask(unittest.TestCase):
    def test_fast(self):
        task_id = aws.schedule_task(
            AWS_FAST_TEST_CLIENT_NAME,
            [0, 5],
        )

        logs_client = boto3.client('logs')
        starttime = time.time()
        while time.time() < starttime + 30:
            response = logs_client.filter_log_events(
                logGroupName=awsconstants.get_worker_sns_topic(AWS_FAST_TEST_CLIENT_NAME),
                logStreamNames=[task_id],
            )

            for event in response['events']:
                try:
                    message = json.loads(event['message'])
                except json.JSONDecodeError:
                    continue

                if message.get('action') == awsconstants.LogActions.COMPLETE:
                    return

            time.sleep(1)

        self.fail("Did not find success marker in logs")


if __name__ == '__main__':
    unittest.main()
