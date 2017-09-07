#!/usr/bin/env python
# coding: utf-8

from __future__ import absolute_import, division, print_function, unicode_literals

import itertools
import os
import sys
import time
import typing
import unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.events import chunkedtask
from dss.events.chunkedtask import Task, aws, _awstest
from tests.chunked_worker import TestStingyRuntime, run_task_to_completion


class TestChunkedTask(chunkedtask.Task[typing.Tuple[int, int, int], typing.Tuple[int, int]]):
    def __init__(
            self,
            state: typing.Tuple[int, int, int],
            expected_max_one_unit_runtime_millis: int) -> None:
        self.x0, self.x1, self.rounds_remaining = state
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

        self.rounds_remaining -= 1

        if self.rounds_remaining == 0:
            return self.x0, self.x1
        return None


class TestChunkedTaskRunner(unittest.TestCase):
    def test_workload_resumes(self):
        initial_state = (1, 1, 25)
        expected_max_one_unit_runtime_millis = 10  # we know exactly how long we'll take.  we're so good at guessing!

        freeze_count, result = run_task_to_completion(
            TestChunkedTask,
            initial_state,
            lambda results: TestStingyRuntime(results, itertools.repeat(sys.maxsize, 19)),
            lambda task_class, task_state, runtime: task_class(task_state, expected_max_one_unit_runtime_millis),
            lambda runtime: runtime.result,
            lambda runtime: runtime.scheduled_work,
        )

        self.assertEqual(result, (196418, 121393))
        self.assertEqual(freeze_count, 2)


class TestForkedTask(unittest.TestCase):
    def test_forked_task_locally(self):
        """
        This is an elaborate test that involves an initial task that spawns another task to complete some work.  Once
        that work is complete, the initial task will complete.

        In this case, all the tasks are run locally.
        """

        class LocalSupervisorTask(_awstest.SupervisorTask):
            def __init__(self, state: dict, runtime: TestStingyRuntime) -> None:
                super().__init__(state, runtime)

            def check_success_marker(self):
                task_id = self.state[_awstest.SupervisorTask.SPAWNED_TASK_KEY]
                if task_id in typing.cast(TestStingyRuntime, self.runtime).global_results:
                    return True

                return None

        def task_creator(task_class: typing.Type[Task], task_state: typing.Any, runtime: chunkedtask.Runtime):
            if task_class == LocalSupervisorTask:
                return LocalSupervisorTask(task_state, typing.cast(TestStingyRuntime, runtime))
            elif task_class == _awstest.AWSFastTestTask:
                return _awstest.AWSFastTestTask(task_state)
            raise ValueError(f"Unknown task class {task_class}")

        initial_state = dict()

        freeze_count, result = run_task_to_completion(
            LocalSupervisorTask,
            initial_state,
            lambda results: TestStingyRuntime(results),
            lambda task_class, task_state, runtime: task_creator(task_class, task_state, runtime),
            lambda runtime: runtime.result,
            lambda runtime: runtime.scheduled_work,
        )

        self.assertGreater(freeze_count, 0)
        self.assertTrue(result is True)


class TestAWSChunkedTask(unittest.TestCase):
    def test_fast(self):
        task_id = aws.schedule_task(_awstest.AWSFastTestTask, [0, 5])

        starttime = time.time()
        while time.time() < starttime + 30:
            if _awstest.is_task_complete(_awstest.AWS_FAST_TEST_CLIENT_NAME, task_id):
                return

            time.sleep(1)

        self.fail("Did not find success marker in logs")


if __name__ == '__main__':
    unittest.main()
