import threading
import typing
from concurrent import futures

"""
This module provides a class that runs a task that is subdivided into a number of sequentially numbered subtasks that
can be executed in parallel.  Every time a subtask is complete, we report progress on the incomplete subtask with the
lowest number subtask id.
"""


class Reporter:
    """
    Abstract base class for reporting progress.  This is called with a lock, so it is protected against multiple
    concurrent callers.
    """
    def report_progress(self, first_incomplete: int) -> None:
        raise NotImplementedError()


class Task:
    """
    Abstract base class for task that is subdivided into sequentially numbered subtasks.  Subclasses should implement
    the `run` method.
    """
    def __init__(self, incomplete: typing.Sequence[int], reporter: Reporter) -> None:
        """
        :param incomplete: The list of incomplete tasks.
        :param reporter: Reporter to report progress.
        """
        sorted_incomplete = sorted(incomplete)
        self._first_incomplete = sorted_incomplete[0]
        self._end = sorted_incomplete[-1]
        self._reporter = reporter

        self._complete: typing.MutableMapping[int, bool] = dict()
        for subtask_id in range(self._first_incomplete, self._end + 1):
            self._complete[subtask_id] = True
        for subtask_id in incomplete:
            self._complete[subtask_id] = False

        self._lock = threading.Lock()

    def run(self, subtask_id: int) -> None:
        raise NotImplementedError()

    def _actual_worker(self, subtask_id: int) -> bool:
        self.run(subtask_id)
        with self._lock:
            self._complete[subtask_id] = True
            for scan_subtask_id in range(self._first_incomplete, self._end + 1):
                if not self._complete[scan_subtask_id]:
                    # found incomplete work.
                    self._first_incomplete = scan_subtask_id
                    break
            else:
                self._first_incomplete = self._end + 1

            self._reporter.report_progress(self._first_incomplete)
        return True


class Runner:
    def __init__(
            self, workers: int,
            task_cls: typing.Type[Task],
            incomplete: typing.Sequence[int],
            reporter: Reporter,
    ) -> None:
        self._workers = workers
        if len(incomplete) != 0:
            self._task = task_cls(incomplete, reporter)
        else:
            self._task = None
        self._incomplete = incomplete

    def run(self) -> typing.Iterator[bool]:
        if len(self._incomplete) == 0:
            return iter([])
        with futures.ThreadPoolExecutor(max_workers=self._workers) as executor:
            return executor.map(self._task._actual_worker, self._incomplete)
