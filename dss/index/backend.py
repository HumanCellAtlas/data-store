import concurrent.futures
from typing import Optional, Type, MutableSet, Union, Iterable, Set

from abc import ABCMeta, abstractmethod
from operator import methodcaller

from dss.index.bundle import Bundle, Tombstone
from dss.util.types import LambdaContext


class IndexBackend(metaclass=ABCMeta):
    """
    An abstract class defining the interface between the data store and a particular document database for the
    purpose of indexing and querying metadata associated with bundles and the files contained in them.
    """
    def __init__(self, context: LambdaContext, dryrun: bool = False, notify: Optional[bool] = True, **kwargs) -> None:
        """
        Create a new index backend.

        :param dryrun: if True, log only, don't make any modifications to the index

        :param notify: False: never notify
                       None: notify on changes
                       True: always notify
        """
        self.dryrun = dryrun
        self.notify = notify
        self.context = context

    @abstractmethod
    def index_bundle(self, bundle: Bundle):
        """
        Update the index with the data from the specified bundle.
        """
        raise NotImplementedError()

    @abstractmethod
    def remove_bundle(self, bundle: Bundle, tombstone: Tombstone):
        """
        Remove a given bundle's data from the index, optionally replacing it with that from the specified tombstone.
        """
        raise NotImplementedError()

    def __repr__(self) -> str:
        return f"{type(self).__name__}(dryrun={self.dryrun}, notify={self.notify})"


class CompositeIndexBackend(IndexBackend):
    """
    An index backend that delegates to multiple underlying backends, concurrently so where applicable.
    """
    def __init__(self,
                 executor: concurrent.futures.ThreadPoolExecutor,
                 backends: Iterable[Union[IndexBackend, Type[IndexBackend]]],
                 timeout=None,
                 *args, **kwargs) -> None:
        """
        :param executor: the executor to be used for delegating operations to all underlying backends in parallel

        :param backends: the backends to delegate to. Can be a mix of backend classes and instances. Any class will be
                         instantiated with args and kwargs, an instance will be used as is.

        :param timeout: see :py:meth:`.timeout`

        :param args: arguments for the constructor of the super class and any backend classes in `backends` (or all
                     registered backend classes if `backends` is None).

        :param kwargs: keyword arguments for the same purpose as `args`
        """
        super().__init__(*args, **kwargs)
        self._timeout = timeout
        self._executor = executor

        def make_backend(backend: Union[IndexBackend, Type[IndexBackend]]) -> IndexBackend:
            if isinstance(backend, IndexBackend):
                return backend
            elif issubclass(backend, IndexBackend):
                return backend(*args, **kwargs)
            else:
                raise ValueError(f"Not an instance or subclass of {IndexBackend.__name__}")

        self._backends = set(map(make_backend, backends))

    @property
    def timeout(self):
        """
        The time in which concurrently executed operations have to be completed by all underlying backends. If a
        backend operation does not complete within the specified timeout, an exception will be raised. A value of
        None disables the timeout, potentially causing the calling thread to block forever.
        """
        return self._timeout

    @timeout.setter
    def timeout(self, timeout):
        """
        Modify the timeout for the next backend operation.
        """
        assert timeout is None or timeout > 0
        self._timeout = timeout

    def index_bundle(self, *args, **kwargs):
        self._delegate(self.index_bundle, args, kwargs)

    def remove_bundle(self, *args, **kwargs):
        self._delegate(self.remove_bundle, args, kwargs)

    def _delegate(self, method, args, kwargs):
        timeout = self._timeout  # defensive copy
        fn = methodcaller(method.__name__, *args, **kwargs)
        future_to_backend = {self._executor.submit(fn, backend): backend
                             for backend in self._backends}
        done, not_done = concurrent.futures.wait(future_to_backend.keys(), timeout=timeout)
        results = {}
        problems = []
        for future in not_done:
            backend = future_to_backend[future]
            problems.append(f"Backend {backend} timed out")
        for future in done:
            exception = future.exception()
            backend = future_to_backend[future]
            if exception is None:
                results[backend] = future.result()
            else:
                problems.append(f"Backend {backend} raised an exception: {exception}")
        if problems:
            raise RuntimeError(f"One or more backends failed: {problems}")
        return results
