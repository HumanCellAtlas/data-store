import json
import logging
from typing import Optional, Mapping, Any, MutableMapping, Type
from urllib.parse import unquote

from abc import ABCMeta, abstractmethod

from dss import Replica, Config
from dss.storage.bundles import ObjectIdentifier, BundleFQID, TombstoneID, ObjectIdentifierError, FileFQID
from dss.storage.index_document import BundleDocument, BundleTombstoneDocument
from dss.util.es import elasticsearch_retry


logger = logging.getLogger(__name__)

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

    def process_new_indexable_object(self, event: Mapping[str, Any]) -> None:
        try:
            key = self._parse_event(event)
            try:
                self.index_object(key)
            except ObjectIdentifierError:
                # This is expected with events about blobs as they don't have a valid object identifier
                logger.debug(f"Not processing {self.replica.name} event for key: {key}")
        except Exception:
            logger.error("Exception occurred while processing %s event: %s",
                         self.replica, json.dumps(event, indent=4), exc_info=True)
            raise

    @elasticsearch_retry(logger)
    def index_object(self, key):
        elasticsearch_retry.add_context(key=key, indexer=self)
        identifier = ObjectIdentifier.from_key(key)
        if isinstance(identifier, BundleFQID):
            self._index_bundle(self.replica, identifier)
        elif isinstance(identifier, TombstoneID):
            self._index_tombstone(self.replica, identifier)
        elif isinstance(identifier, FileFQID):
            logger.debug(f"Indexing of individual files is not supported. "
                         f"Ignoring file {identifier} in {self.replica.name}.")
        else:
            assert False, f"{identifier} is of unknown type"

    @abstractmethod
    def _parse_event(self, event: Mapping[str, Any]):
        raise NotImplementedError()

    def _index_bundle(self, replica: Replica, bundle_fqid: BundleFQID):
        logger.info(f"Indexing bundle {bundle_fqid} from replica {replica.name}.")
        doc = BundleDocument.from_replica(replica, bundle_fqid)
        modified, index_name = doc.index(dryrun=self.dryrun)
        if self.notify or modified and self.notify is None:
            doc.notify(index_name)
        logger.debug(f"Finished indexing bundle {bundle_fqid} from replica {replica.name}.")

    def _index_tombstone(self, replica: Replica, tombstone_id: TombstoneID):
        logger.info(f"Indexing tombstone {tombstone_id} from {replica.name}.")
        doc = BundleTombstoneDocument.from_replica(replica, tombstone_id)
        doc.index(dryrun=self.dryrun)
        logger.info(f"Finished indexing tombstone {tombstone_id} from {replica.name}.")

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
