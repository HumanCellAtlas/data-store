"""Lambda function for DSS indexing"""

import json
import os
import re
import uuid
from urllib.parse import unquote

import boto3
import botocore
import requests
from elasticsearch.helpers import scan

from ... import (DSS_ELASTICSEARCH_INDEX_NAME, DSS_ELASTICSEARCH_DOC_TYPE,
                 DSS_ELASTICSEARCH_QUERY_TYPE, DSS_ELASTICSEARCH_SUBSCRIPTION_INDEX_NAME,
                 DSS_ELASTICSEARCH_SUBSCRIPTION_TYPE)
from ...util import create_blob_key
from ...hcablobstore import BundleMetadata, BundleFileMetadata
from ...util.es import ElasticsearchClient

DSS_BUNDLE_KEY_REGEX = r"^bundles/[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-4[0-9A-Fa-f]{3}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}\..+$"


def process_new_indexable_object(event, logger) -> None:
    try:
        # This function is only called for S3 creation events
        key = unquote(event['Records'][0]["s3"]["object"]["key"])
        if is_bundle_to_index(key):
            logger.info(f"Received S3 creation event for bundle which will be indexed: {key}")
            s3 = boto3.resource('s3')
            bucket_name = event['Records'][0]["s3"]["bucket"]["name"]
            manifest = read_bundle_manifest(s3, bucket_name, key, logger)
            bundle_id = get_bundle_id_from_key(key)
            index_data = create_index_data(s3, bucket_name, bundle_id, manifest, logger)
            add_index_data_to_elasticsearch(bundle_id, index_data, logger)
            subscriptions = find_matching_subscriptions(index_data, logger)
            process_notifications(bundle_id, subscriptions, logger)
            logger.debug(f"Finished index processing of S3 creation event for bundle: {key}")
        else:
            logger.debug(f"Not indexing S3 creation event for key: {key}")
    except Exception as ex:
        logger.error(f"Exception occurred while processing S3 event: {ex} Event: {json.dumps(event, indent=4)}")
        raise


def is_bundle_to_index(key) -> bool:
    # Check for pattern /bundles/<bundle_uuid>.<timestamp>
    # Don't process notifications explicitly for the latest bundle, of the format /bundles/<bundle_uuid>
    # The versioned/timestamped name for this same bundle will get processed, and the fully qualified
    # name will be needed to remove index data later if the bundle is deleted.
    result = re.search(DSS_BUNDLE_KEY_REGEX, key)
    return result is not None


def read_bundle_manifest(s3, bucket_name, bundle_key, logger):
    manifest_string = s3.Object(bucket_name, bundle_key).get()['Body'].read().decode("utf-8")
    logger.debug(f"Read bundle manifest from bucket {bucket_name}"
                 f" with bundle key {bundle_key}: {manifest_string}")
    manifest = json.loads(manifest_string, encoding="utf-8")
    return manifest


def create_index_data(s3, bucket_name, bundle_id, manifest, logger):
    index = dict(state="new", manifest=manifest)
    files_info = manifest[BundleMetadata.FILES]
    index_files = {}
    bucket = s3.Bucket(bucket_name)
    for file_info in files_info:
        if file_info[BundleFileMetadata.INDEXED] is True:
            if file_info[BundleFileMetadata.CONTENT_TYPE] != 'application/json':
                logger.warning(f"In bundle {bundle_id} the file \"{file_info[BundleFileMetadata.NAME]}\""
                               " is marked for indexing yet has content type"
                               f" \"{file_info[BundleFileMetadata.CONTENT_TYPE]}\""
                               " instead of the required content type \"application/json\"."
                               " This file will not be indexed.")
                continue
            try:
                file_key = create_blob_key(file_info)
                file_string = bucket.Object(file_key).get()['Body'].read().decode("utf-8")
                file_json = json.loads(file_string)
            # TODO (mbaumann) Are there other JSON-related exceptions that should be checked below?
            except json.decoder.JSONDecodeError as ex:
                logger.warning(f"In bundle {bundle_id} the file \"{file_info[BundleFileMetadata.NAME]}\""
                               " is marked for indexing yet could not be parsed."
                               f" This file will not be indexed. Exception: {ex}")
                continue
            except botocore.exceptions.ClientError as ex:
                logger.warning(f"In bundle {bundle_id} the file \"{file_info[BundleFileMetadata.NAME]}\""
                               " is marked for indexing yet could not be accessed."
                               f" This file will not be indexed. Exception: {ex}")
                continue
            logger.debug(f"Indexing file: {file_info[BundleFileMetadata.NAME]}")
            # There are two reasons in favor of not using dot in the name of the individual
            # files in the index document, and instead replacing it with an underscore.
            # 1. Ambiguity regarding interpretation/processing of dots in field names,
            #    which could potentially change between Elasticsearch versions. For example, see:
            #       https://github.com/elastic/elasticsearch/issues/15951
            # 2. The ES DSL queries are easier to read when there is no abiguity regarding
            #    dot as a field separator, as may be seen in the Boston demo query.
            # The Boston demo query spec uses underscore instead of dot in the filename portion
            # of the query spec, so go with that, at least for now. Do so by substituting
            # dot for underscore in the key filename portion of the index.
            # As due diligence, additional investigation should be performed.
            index_filename = file_info[BundleFileMetadata.NAME].replace(".", "_")
            index_files[index_filename] = file_json
    index['files'] = index_files
    return index


def get_bundle_id_from_key(bundle_key):
    bundle_prefix = "bundles/"
    if bundle_key.startswith(bundle_prefix):
        return bundle_key[len(bundle_prefix):]
    raise Exception(f"This is not a key for a bundle: {bundle_key}")


def add_index_data_to_elasticsearch(bundle_id, index_data, logger) -> None:
    create_elasticsearch_index(logger)
    logger.debug(f"Adding index data to Elasticsearch: {json.dumps(index_data, indent=4)}")
    add_data_to_elasticsearch(bundle_id, index_data, logger)


def create_elasticsearch_index(logger):
    index_mapping = {
        'mappings': {
            DSS_ELASTICSEARCH_QUERY_TYPE: {
                'properties': {
                    'query': {
                        'type': "percolator"
                    }
                }
            }
        }
    }
    try:
        es_client = ElasticsearchClient.get(logger)
        response = es_client.indices.exists(DSS_ELASTICSEARCH_INDEX_NAME)
        if not response:
            logger.debug(f"Creating new Elasticsearch index: {DSS_ELASTICSEARCH_INDEX_NAME}")
            response = es_client.indices.create(DSS_ELASTICSEARCH_INDEX_NAME, body=index_mapping)
            logger.debug(f"Index creation response: {json.dumps(response, indent=4)}")
        else:
            logger.debug(f"Using existing Elasticsearch index: {DSS_ELASTICSEARCH_INDEX_NAME}", )
    except Exception as ex:
        logger.critical(f"Unable to create index {DSS_ELASTICSEARCH_INDEX_NAME}  Exception: {ex}")
        raise


def add_data_to_elasticsearch(bundle_id, index_data, logger) -> None:
    try:
        ElasticsearchClient.get(logger).index(index=DSS_ELASTICSEARCH_INDEX_NAME,
                                              doc_type=DSS_ELASTICSEARCH_DOC_TYPE,
                                              id=bundle_id,
                                              body=json.dumps(index_data))  # Do not use refresh here - too expensive.
    except Exception as ex:
        logger.error(f"Document not indexed. Exception: {ex}  Index data: {json.dumps(index_data, indent=4)}")
        raise


def find_matching_subscriptions(index_data, logger):
    percolate_document = {
        'query': {
            'percolate': {
                'field': "query",
                'document_type': DSS_ELASTICSEARCH_DOC_TYPE,
                'document': index_data
            }
        }
    }
    subscription_ids = set()
    for hit in scan(ElasticsearchClient.get(logger),
                    index=DSS_ELASTICSEARCH_INDEX_NAME,
                    query=percolate_document):
        subscription_ids.add(hit["_id"])
    logger.debug("Found matching subscription count: %i", len(subscription_ids))
    return subscription_ids


def process_notifications(bundle_id, subscription_ids, logger):
    for subscription_id in subscription_ids:
        try:
            # TODO Batch this request
            subscription = get_subscription(subscription_id, logger)
            notify(subscription_id, subscription, bundle_id, logger)
        except Exception as ex:
            logger.error(f"Error occurred while processing subscription {subscription_id} for bundle {bundle_id}. {ex}")


def get_subscription(subscription_id, logger):
    subscription_query = {
        'query': {
            'ids': {
                'type': DSS_ELASTICSEARCH_SUBSCRIPTION_TYPE,
                'values': [subscription_id]
            }
        }
    }
    response = ElasticsearchClient.get(logger).search(
        index=DSS_ELASTICSEARCH_SUBSCRIPTION_INDEX_NAME,
        body=subscription_query)
    if len(response['hits']['hits']) == 1:
        return response['hits']['hits'][0]['_source']


def notify(subscription_id, subscription, bundle_id, logger):
    bundle_uuid, _, bundle_version = bundle_id.partition(".")
    transaction_id = str(uuid.uuid4())
    payload = {
        "transaction_id": transaction_id,
        "subscription_id": subscription_id,
        "query": subscription['query'],
        "match": {
            "bundle_uuid": bundle_uuid,
            "bundle_version": bundle_version
        }
    }
    # TODO (mbaumann) Ensure webhooks are only delivered over verified HTTPS (unless maybe when running a test)
    callback_url = subscription['callback_url']
    response = requests.post(callback_url, json=payload)
    # TODO (mbaumann) Add webhook retry logic
    if 200 <= response.status_code < 300:
        logger.info(f"Successfully notified for subscription {subscription_id}"
                    f" for bundle {bundle_id} with transaction id {transaction_id} Code: {response.status_code}")
    else:
        logger.warning(f"Failed notification for subscription {subscription_id}"
                       f" for bundle {bundle_id} with transaction id {transaction_id} Code: {response.status_code}")
