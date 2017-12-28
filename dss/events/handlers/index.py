import json
from typing import Optional, Mapping, Any, MutableMapping, Type
from urllib.parse import unquote

from abc import ABCMeta, abstractmethod

from dss import Replica, Config
from dss.storage.bundles import ObjectIdentifier, BundleFQID, TombstoneID
from dss.storage.index_document import BundleDocument, BundleTombstoneDocument
from dss.util.es import elasticsearch_retry


class Indexer(metaclass=ABCMeta):

    def __init__(self, *args, dryrun: bool=False, notify: Optional[bool]=True, **kwargs) -> None:
        """
        :param dryrun: if True, log only, don't make any modifications
        :param notify: False: never notify
                       None: notify on updates
                       True: always notify
        """
        # FIXME (hannes): the variadic arguments allow for this to be used as a mix-in for tests.
        # FIXME (hannes): That's an anti-pattern, so it should be eliminated.
        # noinspection PyArgumentList
        super().__init__(*args, **kwargs)  # type: ignore
        self.dryrun = dryrun
        self.notify = notify

    def process_new_indexable_object(self, event: Mapping[str, Any], logger) -> None:
        try:
            key = self._parse_event(event)
            self.index_object(key, logger)
        except Exception:
            logger.error("%s", f"Exception occurred while processing {self.replica} "
                               f"event: {json.dumps(event, indent=4)}", exc_info=True)
            raise

    @elasticsearch_retry
    def index_object(self, key, logger):
        elasticsearch_retry.add_context(key=key, indexer=self)
        try:
            identifier = ObjectIdentifier.from_key(key)
        except ValueError:
            identifier = None
        if isinstance(identifier, BundleFQID):
            self._index_bundle(self.replica, identifier, logger)
        elif isinstance(identifier, TombstoneID):
            self._index_tombstone(self.replica, identifier, logger)
        else:
            logger.debug("%s", f"Not processing {self.replica.name} event for key: {key}")

    @abstractmethod
    def _parse_event(self, event: Mapping[str, Any]):
        raise NotImplementedError()

    def _index_bundle(self, replica: Replica, bundle_fqid: BundleFQID, logger):
        logger.info("%s", f"Indexing bundle {bundle_fqid} from replica {replica.name}.")
        doc = BundleDocument.from_replica(replica, bundle_fqid, logger)
        modified, index_name = doc.index(dryrun=self.dryrun)
        if self.notify or modified and self.notify is None:
            doc.notify(index_name)
        logger.debug("%s", f"Finished indexing bundle {bundle_fqid} from replica {replica.name}.")

    def _index_tombstone(self, replica: Replica, tombstone_id: TombstoneID, logger):
        logger.info("%s", f"Indexing tombstone {tombstone_id} from {replica.name}.")
        doc = BundleTombstoneDocument.from_replica(replica, tombstone_id, logger)
        doc.index(dryrun=self.dryrun)
        logger.info("%s", f"Finished indexing tombstone {tombstone_id} from {replica.name}.")

    def __repr__(self) -> str:
        return f"{type(self).__name__}(dryrun={self.dryrun}, notify={self.notify})"

    replica: Optional[Replica] = None  # required in concrete subclasses

    for_replica = {}  # type: MutableMapping[Replica, Type['Indexer']]

    def __init_subclass__(cls: Type['Indexer']) -> None:
        super().__init_subclass__()
        assert isinstance(cls.replica, Replica)
        cls.for_replica[cls.replica] = cls


class AWSIndexer(Indexer):

    replica = Replica.aws

    def _parse_event(self, event):
        assert event['Records'][0]['s3']['bucket']['name'] == Config.get_s3_bucket()
        key = unquote(event['Records'][0]['s3']['object']['key'])
        return key


class GCPIndexer(Indexer):

    replica = Replica.gcp

    def _parse_event(self, event):
        key = event['name']
        assert event['bucket'] == Config.get_gs_bucket()
        return key
