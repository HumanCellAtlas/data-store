import typing

TaskStateType = typing.TypeVar('TaskStateType')
TaskResultType = typing.TypeVar('TaskResultType')
RuntimeStateType = typing.TypeVar('RuntimeStateType')
RuntimeResultType = typing.TypeVar('RuntimeResultType')


class Task(typing.Generic[TaskStateType, TaskResultType]):
    def run_one_unit(self) -> typing.Optional[TaskResultType]:
        """
        In implementations of `Task`, this should run one unit of work.  Returns None if there's more work to do, or any
        non-None value if the work is complete.  The returned value is passed to `Runtime.work_complete_callback(..)`.
        """
        raise NotImplementedError()

    @property
    def expected_max_one_unit_runtime_millis(self) -> int:
        """In implementations of `ChunkedTask`, this should return the expected maximum runtime for one unit of work."""
        raise NotImplementedError()

    def get_state(self) -> TaskStateType:
        """
        In implementations of `Task`, this should serialize and return the state of the work.  Passing this to a new
        instance of `Task` should allow the work to resume.
        """
        raise NotImplementedError()


class Runtime(typing.Generic[RuntimeStateType, RuntimeResultType]):
    """
    This is the execution environment that the task is running in.
    """
    def get_remaining_time_in_millis(self) -> int:
        """
        In implementations of `Runtime`, this should return the amount of time left to complete the work in
        the current execution environment.  If insufficient time remains, the state of the task may be serialized and
        scheduled for future execution.
        """
        raise NotImplementedError()

    def schedule_work(
            self,
            task_class: typing.Type[Task[typing.Any, typing.Any]],
            state: typing.Any,
            new_task: bool) -> str:
        """
        In implementations of `Runtime`, this should schedule a new task with a given serialized state.
        """
        raise NotImplementedError()

    def work_complete_callback(self, result: RuntimeResultType):
        """
        Implementations of `Runtime` may implement this if they need to know that the task completed.
        """
        pass
