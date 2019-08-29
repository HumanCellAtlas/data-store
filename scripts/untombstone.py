import os, re
import logging as logger

from hca.dss import DSSClient
from hca.util.exceptions import SwaggerAPIException
from dss.storage.identifiers import DSS_BUNDLE_TOMBSTONE_REGEX as dead_template

#need to import: Blobstore, Config, dynamodb


def parse_keys_to_fqid(key_list, replica)
    """
    Param :: key_list :: list ::  [ f'{bundles OR collections}/{uuid}.{version}.dead',...]
    Param :: replica :: string :: 'aws' or 'gcp' 
    Return :: bundle_list , collection_list, replica :: list, list, string :: [bundle_list] , [collection_list] , aws or gcp
    function that will parse through a list of keys 
    """

    parsed_collection_keys = []
    parsed_bundle_keys = []
    for key in key_list:
        unparsed_key = key
        try:
            handle = Config.get_blobostore_handle(replica)
            handle.get(replica.bucket, unparsed_key)
        except BlobNotFoundError:
            pass
        else: 
            collection_or_bundle, parsed_fqid = unparsed_key.split("/")
            if collection_or_bundle == "bundle":
                parsed_bundle_keys.append(parsed_fqid)
            else:
                parsed_collection_keys.append(parsed_fqid)
    logger.debug("Parsed all keys from {replica}")
    return parsed_collection_keys, parsed_bundle_keys, replica
    
# --------------------------------------------------------------
# bundle / collectione
# --------------------------------------------------------------

def untombstone_bundle_collection(parsed_bundle_keys, parsed_collection_keys, replica):
    """
    Param :: parsed_colletion_keys :: list :: [ f'/{uuid}.{version}.dead',...]
    Param :: parsed_collcetion_keys :: list ::  [ f'{uuid}.{version}.dead',...]
    Param :: replica :: string :: aws or gcp
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
    if len(parsed_collection_keys) > 0:
       for collection_key in parsed_collection_keys:
           if tombstoned_or_not_collection(collecton_key) is False:
               pass
           else:
               deindex_dead_reindex_collection()

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
    else:
        # matches query and deletes all versions with the uuid 
        uuid = fqid.split(".")[0]
        es_client.delete_by_query(
            index="_all",
            body= {"query":{"terms":{"_id":[uuid]}}}
         )
        logger.debug(f"removed dead bundle {uuid} from es")

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
            body= {"query":{"terms":{"_id":[fqid]}}}
        )

    else:
        uuid ,dead_tag = fqid.split(".")
        # brings back all the versions
        es_client.update_by_query(
            index = "_all",
            body= {"query":{"terms":{"_id":["{}.{}".format(uuid, "dead")]}}}
        )

    logger.debug("Untombstoned original bundle {uuid}.{version}")


# --------------------------------------------------------------
# tombstone 
# --------------------------------------------------------------


def tombstoned_or_not_collection(fqid, replica):
    """
    Param :: fqid :: string :: {uuid}.{version}
    Param :: replica :: string :: aws or gcp
    Return :: boolean :: True/False
    Checks for exiting collection 
    """

     if re.match(dead_template, fqid):
        # I assume thisthe version comes with the dead tag
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


def deindex_dead_reindex_collection(fqid, handle, bucket):
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
    logger.debug(f"revmoed collection {fqid} from dynamodb")
    dynamodb_client.put_item(**og_query)
    logger.debug(f"restored original collection {uuid} {version}")
    
