import typing

TaskStateType = typing.TypeVar('TaskStateType')
RuntimeStateType = typing.TypeVar('RuntimeStateType')


class Task(typing.Generic[TaskStateType]):
    def run_one_unit(self) -> bool:
        """
        In implementations of `Task`, this should run one unit of work.  Returns true if there's more work to do.
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


class Runtime(typing.Generic[RuntimeStateType]):
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

    def schedule_work(self, state: RuntimeStateType):
        """
        In implementations of `Runtime` should, this should schedule a task given its serialized state.
        """
        raise NotImplementedError()
