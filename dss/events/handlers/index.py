"""Lambda function for DSS indexing"""
import json
from urllib.parse import unquote

from dss import Replica, Config
from dss.storage.index_document import BundleDocument, BundleTombstoneDocument
from ...storage.bundles import ObjectIdentifier, BundleFQID, TombstoneID


class IndexHandler:

    @classmethod
    def process_new_indexable_object(cls, event, logger) -> None:
        raise NotImplementedError("'process_new_indexable_object' is not implemented!")

    @classmethod
    def _process_new_indexable_object(cls, replica: Replica, key: str, logger):
        identifier = ObjectIdentifier.from_key(key)
        if isinstance(identifier, BundleFQID):
            cls._index_and_notify(replica, identifier, logger)
        elif isinstance(identifier, TombstoneID):
            cls._delete_from_index(replica, identifier, logger)
        else:
            logger.debug(f"Not processing {replica.name} event for key: {key}")

    @staticmethod
    def _index_and_notify(replica: Replica, bundle_fqid: BundleFQID, logger):
        logger.info(f"Received {replica.name} creation event for bundle which will be indexed: {bundle_fqid}")
        document = BundleDocument.from_replica(replica, bundle_fqid, logger)
        index_name = document.prepare_index()
        document.add_to_index(index_name)
        document.notify_matching_subscribers(index_name)
        logger.debug(f"Finished index processing of {replica.name} creation event for bundle: {bundle_fqid}")

    @staticmethod
    def _delete_from_index(replica: Replica, tombstone_id: TombstoneID, logger):
        logger.info(f"Received {replica.name} deletion event with tombstone identifier: {tombstone_id}")
        tombstone_document = BundleTombstoneDocument.from_replica(replica, tombstone_id, logger)
        dead_documents = tombstone_document.list_dead_bundles()
        for document in dead_documents:
            index_name = document.prepare_index()
            document.clear()
            document.update(tombstone_document)
            document.add_to_index(index_name)
            logger.info(f"Deleted from {replica.name} bundle: {document.fqid}")


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
