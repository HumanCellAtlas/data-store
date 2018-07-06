#!/usr/bin/env python
# coding: utf-8
import os
import sys
import threading
import time
import typing
import unittest
from concurrent import futures

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.util import parallel_worker
from tests.infra import testmode


class TestParallelWorker(unittest.TestCase):
    @testmode.standalone
    def test_empty_tasklist(self):
        """Test the case where the tasklist is empty."""
        reporter = RecordingReporter()
        runner = parallel_worker.Runner(8, LatchedTask, [], reporter)
        results = runner.run()
        self.assertEqual(list(results), [])

    @testmode.standalone
    def test_sequential_complete(self, subtasks=5):
        """Sequentially complete tasks."""
        incomplete = list(range(subtasks))
        reporter = RecordingReporter()
        runner = parallel_worker.Runner(8, LatchedTask, incomplete, reporter)
        task: LatchedTask = runner._task
        with ConcurrentContext(runner.run) as context:
            # after each mark_can_run, sleep for a teeny bit to ensure that the reporting has completed.
            for ix in range(subtasks):
                task.mark_can_run(ix)
                time.sleep(0.1)
                self.assertEqual(reporter.progress_reports[-1], ix + 1)

            results = list(context.result())
            self.assertEqual(len(results), subtasks)
            self.assertTrue(all(results))

    @testmode.standalone
    def test_reverse_sequential_complete(self, subtasks=5):
        """Complete tasks in reverse order."""
        incomplete = list(range(subtasks))
        reporter = RecordingReporter()
        runner = parallel_worker.Runner(8, LatchedTask, incomplete, reporter)
        task: LatchedTask = runner._task
        with ConcurrentContext(runner.run) as context:
            # after each mark_can_run, sleep for a teeny bit to ensure that the reporting has completed.
            for ix in range(subtasks - 1, 0, -1):
                task.mark_can_run(ix)
                time.sleep(0.1)
                self.assertEqual(reporter.progress_reports[-1], 0)

            task.mark_can_run(0)
            time.sleep(0.1)
            self.assertEqual(reporter.progress_reports[-1], subtasks)

            results = list(context.result())
            self.assertEqual(len(results), subtasks)
            self.assertTrue(all(results))

    @testmode.standalone
    def test_random_complete(self):
        """Complete tasks in some random order."""
        incomplete      = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]   # noqa: E221
        sequence        = [5, 4, 0, 8, 1, 9, 2, 3, 7, 6]   # noqa: E221
        next_incomplete = [0, 0, 1, 1, 2, 2, 3, 6, 6, 10]  # noqa: E221
        reporter = RecordingReporter()
        runner = parallel_worker.Runner(8, LatchedTask, incomplete, reporter)
        task: LatchedTask = runner._task
        with ConcurrentContext(runner.run) as context:
            for can_run, expected_next_incomplete in zip(sequence, next_incomplete):
                task.mark_can_run(can_run)
                time.sleep(0.1)
                self.assertEqual(reporter.progress_reports[-1], expected_next_incomplete)

            results = list(context.result())
            self.assertEqual(len(results), 10)
            self.assertTrue(all(results))

class ConcurrentContext:
    class _Context:
        def __init__(self, future: futures.Future) -> None:
            self.future = future

        def result(self):
            return self.future.result()

    def __init__(self, method: typing.Callable) -> None:
        self._executor: futures.ThreadPoolExecutor = None
        self._method = method

    def __enter__(self):
        assert self._executor is None
        self._executor = futures.ThreadPoolExecutor(max_workers=1)
        future = self._executor.submit(self._method)
        return ConcurrentContext._Context(future)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._executor.shutdown()


class LatchedTask(parallel_worker.Task):
    """
    This allows us to complete tasks at our own leisure by flipping a semaphore.  The semaphore has a timeout so tests
    don't hang indefinitely.
    """
    def __init__(self, incomplete: typing.Sequence[int], *args, **kwargs) -> None:
        super().__init__(incomplete, *args, **kwargs)
        self._semaphores: typing.MutableMapping[int, threading.Semaphore] = dict()
        for incomplete_task_id in incomplete:
            self._semaphores[incomplete_task_id] = threading.Semaphore(0)

    def run(self, subtask_id: int):
        self._semaphores[subtask_id].acquire(timeout=10)

    def mark_can_run(self, subtask_id: int):
        self._semaphores[subtask_id].release()


class RecordingReporter(parallel_worker.Reporter):
    def __init__(self) -> None:
        self.progress_reports: typing.MutableSequence[int] = list()

    def report_progress(self, first_incomplete: int) -> None:
        self.progress_reports.append(first_incomplete)


if __name__ == '__main__':
    unittest.main()
