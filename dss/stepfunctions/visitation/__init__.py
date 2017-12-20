
import json
import typing
import botocore
from time import time
from uuid import uuid4
from enum import Enum, auto
from dss import Config, Replica
from .. import step_functions_invoke
from cloud_blobstore import BlobPagingError
from .utils import current_walker_executions, walker_execution_name, walker_prefix


class DSSVisitationException(Exception):
    pass


class DSSVisitationExceptionRetry(DSSVisitationException):
    pass


class StatusCode(Enum):
    """
    Status codes that will alter the flow behaviour of the step functions.
    """
    RUNNING = auto()
    SUCCEEDED = auto()
    FAILED = auto()


class VisitationStateMeta(type):
    """
    Metaclass to make it simple for Visitation subclasses to mix in additiona custom state with simple syntax.
    """

    """Step function state specification shared by sentinel and workers"""
    base_state_spec = dict(
        visitation_class_name=None,
        replica=None,
        bucket=None,
        code=None,
        wait_time=1,
    )

    """Step function state specification specific to the sentinel"""
    sentinel_state_spec = dict(
        name=None,
        k_workers=int,
        prefixes=list,
        waiting=None,
        is_sentinel=True,
    )

    """Step function state specification specific to the workers"""
    walker_state_spec = dict(
        prefix=None,
        marker=None,
        token=None,
        k_starts=int,
        k_processed=int,
    )

    def __new__(mcs, name, bases, attrs, **kwargs):
        cls = super().__new__(mcs, name, bases, attrs)

        additional_walker_state_spec = getattr(cls, 'walker_state_spec', dict())
        additional_sentinel_state_spec = getattr(cls, 'sentinel_state_spec', dict())

        setattr(cls, 'walker_state_spec', {
            ** mcs.base_state_spec,
            ** mcs.walker_state_spec,
            ** additional_walker_state_spec
        })

        setattr(cls, 'sentinel_state_spec', {
            ** mcs.base_state_spec,
            ** mcs.sentinel_state_spec,
            ** additional_sentinel_state_spec
        })

        return cls


class Visitation(metaclass=VisitationStateMeta):
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

        for k, default in state_spec.items():
            v = state.get(k, None)

            if v is None:
                if callable(default):
                    v = default()
                else:
                    v = default

            setattr(self, k, v)

        self.logger = logger

    def get_state(self) -> dict:
        """
        Return step function state at the end of each lambda defined in the sentinel and walker step functions.
        """
        return {
            k: getattr(self, k)
            for k in self.state_spec
        }

    @classmethod
    def with_sentinel_state(cls, state: dict, logger) -> 'Visitation':
        """
        Pull in state specific to the sentinel.
        """
        state_spec = cls.sentinel_state_spec
        return cls(state_spec, state, logger)

    @classmethod
    def with_walker_state(cls, state: dict, logger) -> 'Visitation':
        """
        Pull in state specific to the walkers.
        """
        state_spec = cls.walker_state_spec
        return cls(state_spec, state, logger)

    @classmethod
    def start(cls, replica: str, bucket: str, k_workers: int) -> str:
        name = '{}--{}'.format(cls.__name__, str(uuid4()))

        inp = {
            'visitation_class_name': cls.__name__,
            'name': name,
            'replica': replica,
            'bucket': bucket,
            'k_workers': k_workers,
            'is_sentinel': True
        }

        step_functions_invoke('dss-visitation-{stage}', name, inp)

        return name

    def sentinel_muster(self) -> typing.List[str]:
        """
        Start walkers as needed based on pool of prefixes that are waiting to be executed. When all walkers are
        finished, sets state function status code to SUCCEEDED
        """

        if self.waiting is None:  # type: ignore
            self.waiting = self.prefixes.copy()

        execs = current_walker_executions(self.name)

        running = [walker_prefix(e['name'])  # type: ignore
                   for e in execs]

        if len(running) < self.k_workers:
            k_new = min(
                self.k_workers - len(running),
                len(self.waiting)
            )

            for i in range(k_new):
                pfx = self.waiting[-1]
                self._start_walker(pfx)
                running.append(pfx)
                del self.waiting[-1]

        if running:
            self.code = StatusCode.RUNNING.name
        else:
            self.code = StatusCode.SUCCEEDED.name

        return running

    def _start_walker(self, pfx: str) -> str:
        """
        Start a walker step function to work on a prefix.
        """
        name = walker_execution_name(self.name, pfx)
        inp = {
            'visitation_class_name': self.visitation_class_name,
            'replica': self.replica,
            'bucket': self.bucket,
            'prefix': pfx,
        }

        try:
            step_functions_invoke("dss-visitation-{stage}", name, inp)
        except botocore.exceptions.ClientError as ex:
            raise DSSVisitationException() from ex

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

    def process_item(self, key: str) -> None:
        """
        Subclasses must impliment this method. Called once for each blob visited.
        """
        raise NotImplementedError

    def _walk(self, timeout) -> None:
        """
        Subclasses should not typically impliment this method, which includes logic specific to calling
        self.process_item(*args) on each blob visited.
        """

        self.k_starts += 1
        start_time = time()

        handle = Config.get_cloud_specific_handles(Replica[self.replica])[0]

        blobs = handle.list_v2(
            self.bucket,
            prefix=self.prefix,
            start_after_key=self.marker,  # type: ignore  # Cannot determine type of 'marker'
            token=self.token  # type: ignore  # Cannot determine type of 'token'
        )

        self.code = StatusCode.RUNNING.name

        for key in blobs:
            if timeout < time() - start_time:
                break
            self.process_item(key)
            self.k_processed += 1
            self.marker = blobs.start_after_key
            self.token = blobs.token
        else:
            self.code = StatusCode.SUCCEEDED.name

    def walker_walk(self, timeout) -> None:
        try:
            self._walk(timeout)
        except BlobPagingError:
            self.marker = None
            self._walk(timeout)

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
