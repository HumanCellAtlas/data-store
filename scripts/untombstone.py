import os, re
import logging as logger

from dss import Config, Replica
from dss.dynamodb import get_item, put_item, delete_item, DynamoDBItemNotFound
from dss.index.es import ElasticsearchClient
from cloud_blobstore import BlobNotFoundError
from dss.index.es.backend.ElasticsearchIndexBackend import index_bundle
from dss import DSSException, dss_handler, DSSForbiddenException
from dss.collections import owner_lookup

import boto3

from hca.dss import DSSClient
from hca.util.exceptions import SwaggerAPIException
from dss.storage.identifiers import DSS_BUNDLE_TOMBSTONE_REGEX as dead_template

dynamodb_client = boto3.client("dynamodb")

# --------------------------------------------------------------
# untombstone bundles
# --------------------------------------------------------------


def untombstone_bundle(uuid, replica, version=None):
    """
    deletes dead bundles and brings back original bundle 
    """
    if version is not None:
        key = f"{uuid}.{version}.dead"
    else:
        key = f"{uuid}.dead"
    if tombstoned_or_not_bundle(key, replica) is False:
        pass
    else:
        deindex_dead_bundle(key)
        update_og_bundle(key)
        # indexs and notifys subs
        index_bundle(key)


# --------------------------------------------------------------
# bundle
# --------------------------------------------------------------


def tombstoned_or_not_bundle(key, replica):
    """
    Return :: boolean :: True/False
    Checks if bundle is  tombstoned or not 
    """

    try:
        handle = Config.get_blobstore_handle(replica)
        handle.get(replica.bucket, key)
    except BlobNotFoundError:
        raise DSSException(404, "not_found", "Cannot find bundle!")


def deindex_dead_bundle(key):
    """
    Param :: fqid :: string :: {uuid}.{version}
    Removes dead bundle and removes from es
    """

    es_client = ElasticsearchClient.get()

    if re.match(dead_template, key):
        # matches dead fqid and deletes it from es
        es_client.delete_by_query(
            index="_all", body={"query": {"terms": {"_id": [key]}}}
        )
    logger.debug(f"removed dead bundle {key} from es")


def update_og_bundle(fqid):
    """
    Param :: fqid :: string :: {uuid}.{version}
    Finds original bundle and updates it
    """
    es_client = ElasticsearchClient.get()
    if re.match(dead_template, fqid):
        uuid, version, dead_tag = fqid.split(".")
        # brings back the version that was labeled dead
        es_client.update_by_query(
            index="_all",
            body={"query": {"terms": {"_id": ["{}.{}".format(uuid, version)]}}},
        )
        logger.debug("Untombstoned original bundle {uuid}.{version}")

    else:
        uuid, dead_tag = fqid.split(".")
        # brings back all the versions
        es_client.update_by_query(
            index="_all", body={"query": {"terms": {"_id": [uuid]}}}
        )

        logger.debug("Untombstoned original bundles {uuid}")


# --------------------------------------------------------------
# collection
# --------------------------------------------------------------


def untombstone_colelction(uuid, replica, version=None):
    if version is not None:
        key = f"{uuid}.{version}.dead"
    else:
        key = f"{uuid}.dead"

    if tombstoned_or_not_collection(key) is False:
        pass
    else:
        deindex_dead_reindex_collection(key)


def tombstoned_or_not_collection(key):
    """
    Return :: boolean :: True/False
    Checks for exiting collection 
    """

    if re.match(dead_template, key):
        uuid, version = key.split(".", 1)
    else:
        # Just dead tag
        uuid, version = key.split(".")


def deindex_dead_reindex_collection(fqid):
    """
    Finds and deletes dead colletion and restores old collection
    """

    if re.match(dead_template, fqid):
        uuid, version = fqid.split(".", 1)
    else:
        uuid, version = fqid.split(".")

    dead_query = {
        "TableName": owner_lookup.collection_db_table,
        "Key": _format_dynamodb_item(),
    }
    og_query = {
        "TableName": owner_lookup.collection_db_table,
        "Key": _format_dynamodb_item(),
    }

    dynamodb_client.delete_item(**dead_query)
    logger.debug(f"removed collection {fqid} from dynamodb")
    dynamodb_client.put_item(**og_query)
    logger.debug(f"restored original collection {uuid} {version}")
