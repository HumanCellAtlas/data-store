
import copy
import typing
from uuid import uuid4
from enum import Enum, auto
from .. import step_functions_invoke


class DSSVisitationException(Exception):
    pass


class DSSVisitationExceptionRetry(DSSVisitationException):
    pass


class AttrGetter:
    def __getattr__(self, item):
        raise AttributeError(item)


class VisitationStateMeta(type):
    """
    Metaclass to make it simple for Visitation subclasses to mix in additiona custom state with simple syntax.
    """

    """Step function state specification shared by sentinel and workers"""
    common_state_spec = dict(
        _visitation_class_name=None,
        is_finished=False,
    )

    """Step function state specification specific to the sentinel"""
    sentinel_state_spec = dict(
        _processing_work_ids=list,
        _number_of_workers=int,
        work_ids=list,
        wait_time=1,
    )

    """Step function state specification specific to the workers"""
    walker_state_spec = dict(
        work_id=None,
        _sentinel_state_copy=None,
    )

    def __new__(mcs, name, bases, attrs, **kwargs):
        cls = super().__new__(mcs, name, bases, attrs)

        additional_common_state_spec = getattr(cls, 'common_state_spec', dict())
        additional_walker_state_spec = getattr(cls, 'walker_state_spec', dict())
        additional_sentinel_state_spec = getattr(cls, 'sentinel_state_spec', dict())

        common_state_spec = {
            ** mcs.common_state_spec,
            ** additional_common_state_spec,
        }

        setattr(cls, 'walker_state_spec', {
            ** common_state_spec,
            ** mcs.walker_state_spec,
            ** additional_walker_state_spec,
        })

        setattr(cls, 'sentinel_state_spec', {
            ** common_state_spec,
            ** mcs.sentinel_state_spec,
            ** additional_sentinel_state_spec,
        })

        return cls


class Visitation(AttrGetter, metaclass=VisitationStateMeta):
    """
    Base class vor AWS Step Function sentinel-workers datastore batch processing. This is meant to serve as a highly
    parallelized, high throughput architecture to visit blobs in the datastore and perform generic processing.

    Although Visitation is somewhat specialized for datastore processing, subclasses may largely override the propagated
    state and behaviour of the sentinel and walker step functions, hijacking the paralell architecture for
    other purposes.

    subclasses should be registered in registered_visitations to make them available to the sentinel and walker step
    functions.
    """

    def __init__(self, state_spec: dict, state: dict, logger) -> None:
        """
        Pull in fields defined in state specifications and set as instance properties
        """
        self.state_spec = state_spec
        state = copy.deepcopy(state)

        for k, default in state_spec.items():
            v = state.get(k, None)

            if v is None:
                if callable(default):
                    v = default()
                else:
                    v = default

            setattr(self, k, v)

        self.logger = logger

    @classmethod
    def _with_sentinel_state(cls, state: dict, logger) -> 'Visitation':
        """
        Pull in state specific to the sentinel.
        """
        state_spec = cls.sentinel_state_spec
        return cls(state_spec, state, logger)

    @classmethod
    def _with_walker_state(cls, state: dict, logger) -> 'Visitation':
        """
        Pull in state specific to the walkers.
        """
        state_spec = cls.walker_state_spec
        return cls(state_spec, state, logger)

    def get_state(self) -> dict:
        """
        Return step function state at the end of each lambda defined in the sentinel and walker step functions.
        """
        return {
            k: getattr(self, k)
            for k in self.state_spec
        }

    @classmethod
    def start(cls, replica: str, bucket: str, number_of_workers: int) -> str:
        name = '{}--{}'.format(cls.__name__, str(uuid4()))

        inp = {
            '_visitation_class_name': cls.__name__,
            'replica': replica,
            'bucket': bucket,
            '_number_of_workers': number_of_workers,
        }

        step_functions_invoke('dss-visitation-{stage}', name, inp)

        return name

    def sentinel_initialize(self) -> None:
        """
        Impliment for initialization or sanity checking for a sentinel.
        """
        pass

    def sentinel_finalize(self) -> None:
        """
        Impliment for finalization work for a succesful sentinel. Called once each worker has completed.
        """
        pass

    def sentinel_finalize_failed(self) -> None:
        """
        Impliment for finalization work for a failed sentinel. This is your opportunity to cry, notify, and ruminate.
        """
        pass

    def walker_initialize(self) -> None:
        """
        Impliment this method for initialization or sanity checking specifically for a walker.
        """
        pass

    def walker_walk(self) -> None:
        """
        Subclasses must impliment this method. Called for walker thread.
        """
        raise NotImplementedError

    def walker_finalize(self) -> None:
        """
        Impliment this method for finalization work specific to a walker.
        """
        pass

    def walker_finalize_failed(self) -> None:
        """
        Imliment this method for finaliation work specific to a failed walker.
        """
        pass

    def __setattr__(self, key, val):  # typing.type: (str, typing.Any) -> None
        """
        Keep mypy happy with dynamic properties
        """
        super().__setattr__(key, val)

    def __getattr__(self, key):  # typing.type: (str) -> typing.Any
        """
        Keep mypy happy with dynamic properties
        """
        return super().__getattr__(key)
