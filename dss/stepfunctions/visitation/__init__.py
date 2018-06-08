import logging
from typing import Sequence, Any, Mapping, MutableMapping

import copy
import json
from uuid import uuid4
from enum import Enum, auto

from dss.stepfunctions import _step_functions_start_execution
from dss.util.time import RemainingTime
from dss.util.types import JSON

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


Spec = Mapping[str, Any]


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
        execution_name=str,
        work_ids=list,
        work_id=str,
        work_result=None
    )
    state_spec: Spec = dict()
    walker_state_spec: Spec = dict()

    def __init__(self, state_spec: Spec, state: Spec, remaining_time: RemainingTime) -> None:
        """
        Pull in fields defined in state specifications and set as instance properties
        """
        self.state_spec = state_spec
        self._remaining_time = remaining_time
        state = copy.deepcopy(state)

        self.work_result: MutableMapping[str, Any] = None

        for k, default in state_spec.items():
            v = state.get(k, None)

            if v is None:
                if callable(default):
                    v = default()
                else:
                    v = copy.deepcopy(default)

            setattr(self, k, v)

    @classmethod
    def _with_state(cls, state: dict, remaining_time: RemainingTime) -> 'Visitation':
        """
        Pull in state specific to the job.
        """
        state_spec = {
            ** Visitation._state_spec,
            ** cls.state_spec,
            ** cls.walker_state_spec,
        }
        return cls(state_spec, state, remaining_time)

    def get_state(self) -> dict:
        """
        Return step function state at the end of each lambda defined in the job and walker step functions.
        """
        return {
            k: getattr(self, k)
            for k in self.state_spec
        }

    @classmethod
    def start(cls, number_of_workers: int, **kwargs) -> JSON:
        name = '{}--{}'.format(cls.__name__, str(uuid4()))
        execution_input = {
            **kwargs,
            '_visitation_class_name': cls.__name__,
            '_number_of_workers': number_of_workers,
            'execution_name': name
        }
        # Invoke directly without reaper/retry
        execution = _step_functions_start_execution('dss-visitation-{stage}', name, json.dumps(execution_input))
        return dict(arn=execution['executionArn'], name=name, input=execution_input)

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
        return self._remaining_time.get()

    # See MyPy recomendations for silencing spurious warnings of missing properties that have been mixed in:
    # https://mypy.readthedocs.io/en/latest/cheat_sheet.html#when-you-re-puzzled-or-when-things-are-complicated

    def __getattribute__(self, name: str) -> Any:
        return super().__getattribute__(name)

    def __setattr__(self, key: str, val: Any) -> None:
        super().__setattr__(key, val)
