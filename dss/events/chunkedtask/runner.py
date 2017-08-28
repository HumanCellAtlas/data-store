import typing

from .base import Task, Runtime
from .constants import TIME_OVERHEAD_FACTOR

RunnerStateType = typing.TypeVar('RunnerStateType')
RunnerResultType = typing.TypeVar('RunnerResultType')


class Runner(typing.Generic[RunnerStateType, RunnerResultType]):
    """
    This utilizes a given `Runtime` to execute a `Task` as long as it can.  Once there is doubt that the task can
    finish, the task's state is serialized and the system attempts to schedule the continuation of the work.
    """
    def __init__(
            self,
            chunkedtask: Task[RunnerStateType, RunnerResultType],
            runtime: Runtime[RunnerStateType, RunnerResultType]) -> None:
        self.chunkedtask = chunkedtask
        self.runtime = runtime
        self.observed_max_one_unit_runtime_millis = self.chunkedtask.expected_max_one_unit_runtime_millis

    def run(self) -> None:
        """
        Runs a given chunked task in a runtime environment until we either complete the task or we run out of time.  If
        we run out of time, we request that the remaining work be rescheduled.
        """
        while True:
            before = self.runtime.get_remaining_time_in_millis()
            result = self.chunkedtask.run_one_unit()
            if result is not None:
                self.runtime.work_complete_callback(result)
                return
            after = self.runtime.get_remaining_time_in_millis()

            duration = before - after
            if duration > self.observed_max_one_unit_runtime_millis:
                self.observed_max_one_unit_runtime_millis = duration
            else:
                # TODO: this formula may need some tweaking.
                self.observed_max_one_unit_runtime_millis = (self.observed_max_one_unit_runtime_millis + duration) // 2

            if after < self.observed_max_one_unit_runtime_millis * TIME_OVERHEAD_FACTOR:
                break

        # schedule the next chunk of work.
        state = self.chunkedtask.get_state()
        self.runtime.schedule_work(state)
