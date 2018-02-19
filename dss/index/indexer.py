import json
import logging
from typing import Any, Mapping, MutableMapping, Optional, Type
from urllib.parse import unquote

from abc import ABCMeta, abstractmethod

from dss import Config, Replica
from dss.storage.identifiers import BundleFQID, FileFQID, ObjectIdentifier, ObjectIdentifierError, TombstoneID
from .backend import IndexBackend
from .bundle import Bundle, Tombstone

logger = logging.getLogger(__name__)


class Indexer(metaclass=ABCMeta):

    def __init__(self, backend: IndexBackend) -> None:
        super().__init__()
        self.backend = backend

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

    def index_object(self, key):
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
        bundle = Bundle.from_replica(replica, bundle_fqid)
        tombstone = bundle.lookup_tombstone()
        if tombstone is None:
            self.backend.index_bundle(bundle)
        else:
            logger.info(f"Found tombstone for {bundle_fqid} in replica {replica.name}. "
                        f"Indexing tombstone in place of bundle.")
            self.backend.remove_bundle(bundle, tombstone)
        logger.debug(f"Finished indexing bundle {bundle_fqid} from replica {replica.name}.")

    def _index_tombstone(self, replica: Replica, tombstone_id: TombstoneID):
        logger.info(f"Indexing tombstone {tombstone_id} from {replica.name}.")
        tombstone = Tombstone.from_replica(replica, tombstone_id)
        bundles = tombstone.list_dead_bundles()
        for bundle in bundles:
            self.backend.remove_bundle(bundle, tombstone)
        logger.info(f"Finished indexing tombstone {tombstone_id} from {replica.name}.")

    replica: Optional[Replica] = None  # required in concrete subclasses

    @classmethod
    def for_replica(cls, replica: Replica):
        return cls._for_replica[replica]

    _for_replica = {}  # type: MutableMapping[Replica, Type['Indexer']]

    def __init_subclass__(cls: Type['Indexer']) -> None:
        super().__init_subclass__()
        assert isinstance(cls.replica, Replica)
        cls._for_replica[cls.replica] = cls


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
