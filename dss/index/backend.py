import concurrent.futures
from typing import Optional, Type, Union, Iterable

from abc import ABCMeta, abstractmethod
from operator import methodcaller

from dss.index.bundle import Bundle, Tombstone


class IndexBackend(metaclass=ABCMeta):
    """
    An abstract class defining the interface between the data store and a particular document database for the
    purpose of indexing and querying metadata associated with bundles and the files contained in them.
    """

    def __init__(self, dryrun: bool = False, notify: Optional[bool] = True, **kwargs) -> None:
        """
        Create a new index backend.

        :param dryrun: if True, log only, don't make any modifications to the index

        :param notify: False: never notify
                       None: notify on changes
                       True: always notify
        """
        self.dryrun = dryrun
        self.notify = notify

    @abstractmethod
    def estimate_indexing_time(self) -> float:
        """
        Return an upper bound on the time needed for a call to one of the indexing methods.
        """
        raise NotImplementedError()

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
                 *args, **kwargs) -> None:
        """
        :param executor: the executor to be used for delegating operations to all underlying backends in parallel

        :param backends: the backends to delegate to. Can be a mix of backend classes and instances. Any class will be
                         instantiated with args and kwargs, an instance will be used as is.

        :param args: arguments for the constructor of the super class and any backend classes in `backends` (or all
                     registered backend classes if `backends` is None).

        :param kwargs: keyword arguments for the same purpose as `args`
        """
        super().__init__(*args, **kwargs)
        self._executor = executor

        def make_backend(backend: Union[IndexBackend, Type[IndexBackend]]) -> IndexBackend:
            if isinstance(backend, IndexBackend):
                return backend
            elif issubclass(backend, IndexBackend):
                return backend(*args, **kwargs)
            else:
                raise ValueError(f"Not an instance or subclass of {IndexBackend.__name__}")

        self._backends = set(map(make_backend, backends))

    def estimate_indexing_time(self) -> float:
        return max(backend.estimate_indexing_time() for backend in self._backends)

    def index_bundle(self, *args, **kwargs):
        self._delegate(self.index_bundle, args, kwargs)

    def remove_bundle(self, *args, **kwargs):
        self._delegate(self.remove_bundle, args, kwargs)

    def _delegate(self, method, args, kwargs):
        fn = methodcaller(method.__name__, *args, **kwargs)
        future_to_backend = {self._executor.submit(fn, backend): backend
                             for backend in self._backends}
        done, not_done = concurrent.futures.wait(future_to_backend.keys(), timeout=self.estimate_indexing_time())
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
