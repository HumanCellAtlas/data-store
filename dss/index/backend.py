from typing import Optional

from abc import ABCMeta, abstractmethod

from dss.index.bundle import Bundle, Tombstone


class IndexBackend(metaclass=ABCMeta):
    """
    An abstract class defining the interface between the data store and a particular document database for the
    purpose of indexing and querying metadata associated with bundles and the files contained in them.
    """
    def __init__(self, dryrun: bool = False, notify: Optional[bool] = True) -> None:
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
