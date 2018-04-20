import logging
from typing import Sequence, Any, Union

import copy
import json
from uuid import uuid4
from enum import Enum, auto

from dss.stepfunctions import _step_functions_start_execution

logger = logging.getLogger(__name__)


class DSSVisitationException(Exception):
    pass


class DSSVisitationExceptionRetry(DSSVisitationException):
    pass


class WalkerStatus(Enum):
    init = auto()
    walk = auto()
    finished = auto()
    end = auto()


class Visitation:
    """
    Base class vor AWS Step Function job-workers datastore batch processing. This is meant to serve as a highly
    parallelized, high throughput architecture to visit blobs in the datastore and perform generic processing.

    Although Visitation is somewhat specialized for datastore processing, subclasses may largely override the
    propagated state and behaviour of the job and walker step functions, hijacking the parallel architecture for
    other purposes.

    Subclasses should be registered in registered_visitations to make them available to the job and walker step
    functions.
    """

    """Step function state specification shared by job and workers"""
    _state_spec = dict(
        _visitation_class_name=str,
        _status=WalkerStatus.init.name,
        _number_of_workers=int,
        work_ids=list,
        work_id=str,
        work_result=None
    )
    state_spec: dict = dict()
    walker_state_spec: dict = dict()

    def __init__(self, state_spec: dict, state: dict, context) -> None:
        """
        Pull in fields defined in state specifications and set as instance properties
        """
        self.state_spec = state_spec
        self._context = context
        state = copy.deepcopy(state)

        self.work_result: Union[Any, Sequence] = None  # mypy needs this

        for k, default in state_spec.items():
            v = state.get(k, None)

            if v is None:
                if callable(default):
                    v = default()
                else:
                    v = copy.deepcopy(default)

            setattr(self, k, v)

    @classmethod
    def _with_state(cls, state: dict, context) -> 'Visitation':
        """
        Pull in state specific to the job.

        For documentation on the context object refer to

        https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html
        """
        state_spec = {
            ** Visitation._state_spec,
            ** cls.state_spec,
            ** cls.walker_state_spec,
        }
        return cls(state_spec, state, context)

    def get_state(self) -> dict:
        """
        Return step function state at the end of each lambda defined in the job and walker step functions.
        """
        return {
            k: getattr(self, k)
            for k in self.state_spec
        }

    @classmethod
    def start(cls, replica: str, bucket: str, number_of_workers: int, **kwargs) -> str:
        name = '{}--{}'.format(cls.__name__, str(uuid4()))

        inp = {
            **kwargs,
            '_visitation_class_name': cls.__name__,
            'replica': replica,
            'bucket': bucket,
            '_number_of_workers': number_of_workers,
        }
        # Invoke directly without reaper/retry
        _step_functions_start_execution('dss-visitation-{stage}', name, json.dumps(inp))

        return name

    def job_initialize(self) -> None:
        """
        Implement for initialization or sanity checking for a job.
        """
        pass

    def job_finalize(self) -> None:
        """
        Implement for finalization work for a successful job. Called once each worker has completed. The default
        implementation aggregates the work results.
        """
        work_result = self.work_result
        if isinstance(work_result, Sequence):
            work_result = self._aggregate(work_result)
            self.work_result = work_result

    def _aggregate(self, work_result: Sequence) -> Any:
        """
        Aggregates the given work results and returns the aggregate. Subclasses may want to override this method in
        order to customize how work results are aggregated. The default implementation returns the argument.
        """
        return work_result

    def job_finalize_failed(self) -> None:
        """
        Implement for finalization work for a failed job. This is your opportunity to cry, notify, and ruminate.
        """
        pass

    def walker_initialize(self) -> None:
        """
        Implement this method for initialization or sanity checking specifically for a walker.
        """
        pass

    def walker_walk(self) -> None:
        """
        Subclasses must implement this method. Called for walker thread.
        """
        raise NotImplementedError

    def walker_finalize(self) -> None:
        """
        Implement this method for finalization work specific to a walker.
        """
        pass

    def walker_finalize_failed(self) -> None:
        """
        Aliment this method for finalization work specific to a failed walker.
        """
        pass

    def remaining_runtime(self) -> float:
        return self._context.get_remaining_time_in_millis() / 1000

    # See MyPy recomendations for silencing spurious warnings of missing properties that have been mixed in:
    # https://mypy.readthedocs.io/en/latest/cheat_sheet.html#when-you-re-puzzled-or-when-things-are-complicated

    def __getattribute__(self, name: str) -> Any:
        return super().__getattribute__(name)

    def __setattr__(self, key: str, val: Any) -> None:
        super().__setattr__(key, val)
