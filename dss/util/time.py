from abc import ABCMeta, abstractmethod
import time

from dss.util import require
from dss.util.types import LambdaContext


class RemainingTime(metaclass=ABCMeta):
    """
    A monotonically decreasing, non-negative estimate of time remaining in a particular context
    """

    @abstractmethod
    def get(self) -> float:
        """
        Returns the estimated remaining time in seconds
        """
        raise NotImplementedError()


class RemainingLambdaContextTime(RemainingTime):
    """
    The estimated running time in an AWS Lambda context
    """

    def __init__(self, context: LambdaContext) -> None:
        super().__init__()
        self._context = context

    def get(self) -> float:
        return self._context.get_remaining_time_in_millis() / 1000


class RemainingTimeUntil(RemainingTime):
    """
    The remaining wall clock time up to a given absolute deadline in terms of time.time()
    """

    def __init__(self, deadline: float) -> None:
        super().__init__()
        self._deadline = deadline

    def get(self) -> float:
        return max(0.0, self._deadline - time.time())


class SpecificRemainingTime(RemainingTimeUntil):
    """
    A specific relative amount of wall clock time in seconds
    """

    def __init__(self, amount: float) -> None:
        require(amount >= 0, "Inital remaining time must be non-negative")
        super().__init__(time.time() + amount)


class AdjustedRemainingTime(RemainingTime):
    """
    Some other estimate of remaining time, adjusted by a fixed offset. Use a negative offset to reduce the remaining
    time or a positive offset to increase it.
    """

    def __init__(self, offset: float, actual: RemainingTime) -> None:
        super().__init__()
        self._offset = offset
        self._actual = actual

    def get(self) -> float:
        return max(0.0, self._actual.get() + self._offset)
