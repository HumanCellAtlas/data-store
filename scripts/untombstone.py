import os, re
import logging as logger

from dss import Config, Replica
from dss.dynamodb import get_item, put_item, delete_item, DynamoDBItemNotFound

from cloud_blobstore import BlobNotFoudError, BlobStoreError
from hca.dss import DSSClient
from hca.util.exceptions import SwaggerAPIException
from dss.storage.identifiers import DSS_BUNDLE_TOMBSTONE_REGEX as dead_template


# --------------------------------------------------------------
# untombstone bundles
# --------------------------------------------------------------

def untombstone_bundle(parsed_bundle_keys, replica):
    """
    deletes dead bundles and brings back original bundle 
    """

    if len(parsed_bundle_keys) > 0:
        for bundle_key in parsed_bundle_keys:
            if tombstoned_or_not_bundle(bundle_key) is False:
                pass
            else:
                deindex_dead_bundle()
                update_og_bundle()
                es_client.reindex()

# --------------------------------------------------------------
# bundle
# --------------------------------------------------------------

def tombstoned_or_not_bundle(fqid, replica):
    """
    Param :: fqid :: string :: {uuid}.{version}
    Param :: replica :: string :: aws or gcp
    Return :: boolean :: True/False
    Checks if bundle is  tombstoned or not 
    """
    if re.match(dead_template, fqid):
        uuid, version_and_dead = fqid.split(".", 1)
        bundle_query = {"query":{"bool":{"must":[{"match":{"uuid":uuid}},{"match":{"version":version_and_dead}},]}}}
    else:
        uuid, dead_tag = fqid.split(".")
        bundle_query = {"query":{"bool":{"must":[{"match":{"uuid":uuid}},{"match":{"version":dead_tag}},]}}}
    try:
        dss_client.get_bundle(replica=replica, es_query=bundle_query, output_format="raw")
        return True
    except SwaggerAPIException as e:
       if e.code = "404" or e.code = "400":
          return False


def deindex_dead_bundle(fqid):
    """
    Param :: fqid :: string :: {uuid}.{version}
    Removes dead bundle and removes from es
    """
    es_client = get_es_client()

    if re.match(dead_template, fqid):
        # matches dead fqid and deletes it from es
         es_client.delete_by_query(
            index="_all",
            body= {"query":{"terms":{"_id":[fqid]}}}
         )
        logger.debug(f"removed dead bundle {fqid} from es")

def update_og_bundle(fqid):
    """
    Param :: fqid :: string :: {uuid}.{version}
    Finds original bundle and updates it
    """

    if re.match(dead_template, fqid):
        uuid, version, dead_tag = fqid.split(".")
        # brings back the version that was labeled dead
        es_client.update_by_query(
            index = "_all",
            body= {"query":{"terms":{"_id":[uuid]}}}
        )
        logger.debug("Untombstoned original bundle {uuid}.{version}")

    else:
        uuid ,dead_tag = fqid.split(".")
        # brings back all the versions
        es_client.update_by_query(
            index = "_all",
            body= {"query":{"terms":{"_id":["{}".format(uuid)]}}}
        )

        logger.debug("Untombstoned original bundles {uuid}")


# --------------------------------------------------------------
# collection
# --------------------------------------------------------------

def untombstone_colelction(fqid, replica):
    if tombstoned_or_not_collection() is False:
        pass
    else:
        deindex_dead_reindex_collection()

def tombstoned_or_not_collection(fqid, replica):
    """
    Param :: fqid :: string :: {uuid}.{version}
    Param :: replica :: string :: aws or gcp
    Return :: boolean :: True/False
    Checks for exiting collection 
    """

     if re.match(dead_template, fqid):
        uuid, version = fqid.split(".", 1)
    else:
        # Just dead tag 
        uuid, version = fqid.split(".")
    try:
        dss_client.get_collection(replica=replica, uuid=uuid, version=version)
        return True
    except SwaggerAPIException as e:
       if e.code = "404" or e.code = "400":
          return False


def deindex_dead_reindex_collection(fqid, bucket):
    """
    Finds and deletes dead colletion and restores old collection
    """

    if re.match(dead_template, fqid):
        uuid, version = fqid.split(".", 1)
    else:
        uuid, version = fqid.split(".")
    
    dead_query = { "TableName": collection_db_table,
                   "Key": _format_dynamodb_item()
                 }
    og_query   = { "TableName": collection_db_table,
                   "Key": _format_dynamodb_item()
                 }

    dynamodb_client.delete_item(**dead_query)
    logger.debug(f"removed collection {fqid} from dynamodb")
    dynamodb_client.put_item(**og_query)
    logger.debug(f"restored original collection {uuid} {version}")
    
