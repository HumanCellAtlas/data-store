import json
from urllib.parse import unquote

from dss import Replica, Config
from dss.storage.index_document import BundleDocument, BundleTombstoneDocument
from dss.storage.bundles import ObjectIdentifier, BundleFQID, TombstoneID


class IndexHandler:

    @classmethod
    def process_new_indexable_object(cls, event, logger) -> None:
        raise NotImplementedError()

    @classmethod
    def _process_new_indexable_object(cls, replica: Replica, key: str, logger):
        try:
            identifier = ObjectIdentifier.from_key(key)
        except ValueError:
            identifier = None
        if isinstance(identifier, BundleFQID):
            cls._handle_bundle(replica, identifier, logger)
        elif isinstance(identifier, TombstoneID):
            cls._handle_tombstone(replica, identifier, logger)
        else:
            logger.debug(f"Not processing {replica.name} event for key: {key}")

    @staticmethod
    def _handle_bundle(replica: Replica, bundle_fqid: BundleFQID, logger):
        logger.info(f"Indexing bundle {bundle_fqid} from replica {replica.name}.")
        doc = BundleDocument.from_replica(replica, bundle_fqid, logger)
        doc.index_and_notify()
        logger.debug(f"Finished indexing bundle {bundle_fqid} from replica {replica.name}.")

    @staticmethod
    def _handle_tombstone(replica: Replica, tombstone_id: TombstoneID, logger):
        logger.info(f"Indexing tombstone {tombstone_id} from {replica.name}.")
        doc = BundleTombstoneDocument.from_replica(replica, tombstone_id, logger)
        doc.index()
        logger.info(f"Finished indexing tombstone {tombstone_id} from {replica.name}.")


class AWSIndexHandler(IndexHandler):

    @classmethod
    def process_new_indexable_object(cls, event, logger) -> None:
        try:
            # This function is only called for S3 creation events
            key = unquote(event['Records'][0]['s3']['object']['key'])
            assert event['Records'][0]['s3']['bucket']['name'] == Config.get_s3_bucket()
            cls._process_new_indexable_object(Replica.aws, key, logger)
        except Exception as ex:
            logger.error("Exception occurred while processing S3 event: %s Event: %s", ex, json.dumps(event, indent=4))
            raise


class GCPIndexHandler(IndexHandler):

    @classmethod
    def process_new_indexable_object(cls, event, logger) -> None:
        try:
            # This function is only called for GS creation events
            key = event['name']
            assert event['bucket'] == Config.get_gs_bucket()
            cls._process_new_indexable_object(Replica.gcp, key, logger)
        except Exception as ex:
            logger.error("Exception occurred while processing GS event: %s Event: %s", ex, json.dumps(event, indent=4))
            raise
