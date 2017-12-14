"""Lambda function for DSS indexing"""
import json
import re
from urllib.parse import unquote

from dss import Replica, Config
from dss.storage.index_document import BundleDocument, BundleTombstoneDocument
from ...storage.bundles import DSS_BUNDLE_TOMBSTONE_REGEX, DSS_BUNDLE_KEY_REGEX


class IndexHandler:

    @classmethod
    def process_new_indexable_object(cls, event, logger) -> None:
        raise NotImplementedError("'process_new_indexable_object' is not implemented!")

    @classmethod
    def _process_new_indexable_object(cls, replica: Replica, key: str, logger):
        if cls._is_bundle_to_index(key):
            cls._index_and_notify(replica, key, logger)
        elif cls._is_deletion(key):
            cls._delete_from_index(replica, key, logger)
        else:
            logger.debug(f"Not processing {replica.name} event for key: {key}")

    # add to index and notify
    @staticmethod
    def _is_bundle_to_index(key: str) -> bool:
        # Check for pattern /bundles/<bundle_uuid>.<timestamp>
        # Don't process notifications explicitly for the latest bundle, of the format /bundles/<bundle_uuid>
        # The versioned/timestamped name for this same bundle will get processed, and the fully qualified
        # name will be needed to remove index data later if the bundle is deleted.
        result = re.search(DSS_BUNDLE_KEY_REGEX, key)
        return result is not None

    @staticmethod
    def _index_and_notify(replica: Replica, key: str, logger):
        logger.info(f"Received {replica.name} creation event for bundle which will be indexed: {key}")
        document = BundleDocument.from_replica(replica, key, logger)
        index_name = document.prepare_index()
        document.add_to_index(index_name)
        document.notify_matching_subscribers(index_name)
        logger.debug(f"Finished index processing of {replica.name} creation event for bundle: {key}")

    # deletion
    @staticmethod
    def _is_deletion(key: str) -> bool:
        # Check for pattern /bundles/<bundle_uuid>(.<timestamp>)?.dead
        result = re.match(DSS_BUNDLE_TOMBSTONE_REGEX, key)
        return result is not None

    @staticmethod
    def _delete_from_index(replica: Replica, key: str, logger):
        tombstone_document = BundleTombstoneDocument.from_replica(replica, key, logger)
        dead_documents = tombstone_document.list_dead_bundles()
        for document in dead_documents:
            index_name = document.prepare_index()
            document.clear()
            document.update(tombstone_document)
            document.add_to_index(index_name)


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
