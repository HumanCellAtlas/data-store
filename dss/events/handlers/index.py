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
        logger.info(f"Indexing bundle {bundle_fqid} from replica '{replica.name}'.")
        doc = BundleDocument.from_replica(replica, bundle_fqid, logger)
        index_name = doc.prepare_index()
        versions = doc.get_indexed_versions()
        old_version = versions.pop(index_name, None)
        if versions:
            logger.warning(f"Removing stale copies of the bundle document for {bundle_fqid} from the following "
                           f"index(es): {json.dumps(versions)}.")
            doc.remove_versions(versions)
        if old_version:
            old_doc = doc.from_index(replica, bundle_fqid, index_name, logger, version=old_version)
            if doc == old_doc:
                logger.info(f"Document for bundle {bundle_fqid} is already up-to-date in index {index_name} at "
                            f"version {old_version}.")
            else:
                logger.warning(f"Updating an older copy of the document for bundle {bundle_fqid} in index "
                               f"{index_name} at version {old_version}.")
                doc.add_to_index(index_name)
        else:
            logger.info(f"Writing the document for bundle {bundle_fqid} in index "
                        f"{index_name} for the first time.")
            doc.add_to_index(index_name)
        doc.notify_matching_subscribers(index_name)
        logger.debug(f"Finished indexing bundle {bundle_fqid} from replica '{replica.name}'.")

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
