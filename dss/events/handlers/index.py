"""Lambda function for DSS indexing"""

import json
import os
import re
import uuid
import ipaddress
import socket
from urllib.parse import urlparse, unquote

import requests
from cloud_blobstore import BlobStore, BlobStoreError
from elasticsearch.helpers import scan
from requests_http_signature import HTTPSignatureAuth

from dss import Config, DeploymentStage, ESIndexType, ESDocType, Replica
from ...util import create_blob_key
from ...hcablobstore import BundleMetadata, BundleFileMetadata
from ...util.es import ElasticsearchClient, get_elasticsearch_index

DSS_BUNDLE_KEY_REGEX = r"^bundles/[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-4[0-9A-Fa-f]{3}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}\..+$"


def process_new_s3_indexable_object(event, logger) -> None:
    try:
        # This function is only called for S3 creation events
        key = unquote(event['Records'][0]["s3"]["object"]["key"])
        bucket_name = event['Records'][0]["s3"]["bucket"]["name"]
        process_new_indexable_object(bucket_name, key, "aws", logger)
    except Exception as ex:
        logger.error("Exception occurred while processing S3 event: %s Event: %s", ex, json.dumps(event, indent=4))
        raise


def process_new_gs_indexable_object(event, logger) -> None:
    try:
        # This function is only called for GS creation events
        bucket_name = event["bucket"]
        key = event["name"]
        process_new_indexable_object(bucket_name, key, "gcp", logger)
    except Exception as ex:
        logger.error("Exception occurred while processing GS event: %s Event: %s", ex, json.dumps(event, indent=4))
        raise


def process_new_indexable_object(bucket_name: str, key: str, replica: str, logger) -> None:
    if is_bundle_to_index(key):
        logger.info(f"Received {replica} creation event for bundle which will be indexed: {key}")
        blobstore = Config.get_cloud_specific_handles(replica)[0]
        manifest = read_bundle_manifest(blobstore, bucket_name, key, logger)
        bundle_id = get_bundle_id_from_key(key)
        index_data = create_index_data(blobstore, bucket_name, bundle_id, manifest, logger)
        index_name = Config.get_es_index_name(ESIndexType.docs, Replica[replica])
        add_index_data_to_elasticsearch(bundle_id, index_data, index_name, logger)
        subscriptions = find_matching_subscriptions(index_data, index_name, logger)
        process_notifications(bundle_id, subscriptions, replica, logger)
        logger.debug(f"Finished index processing of {replica} creation event for bundle: {key}")
    else:
        logger.debug(f"Not indexing {replica} creation event for key: {key}")


def is_bundle_to_index(key: str) -> bool:
    # Check for pattern /bundles/<bundle_uuid>.<timestamp>
    # Don't process notifications explicitly for the latest bundle, of the format /bundles/<bundle_uuid>
    # The versioned/timestamped name for this same bundle will get processed, and the fully qualified
    # name will be needed to remove index data later if the bundle is deleted.
    result = re.search(DSS_BUNDLE_KEY_REGEX, key)
    return result is not None


def read_bundle_manifest(handle: BlobStore, bucket_name: str, bundle_key: str, logger) -> dict:
    manifest_string = handle.get(bucket_name, bundle_key).decode("utf-8")
    logger.debug(f"Read bundle manifest from bucket {bucket_name}"
                 f" with bundle key {bundle_key}: {manifest_string}")
    manifest = json.loads(manifest_string, encoding="utf-8")
    return manifest


def create_index_data(handle: BlobStore, bucket_name: str, bundle_id: str, manifest: dict, logger) -> dict:
    index = dict(state="new", manifest=manifest)
    files_info = manifest[BundleMetadata.FILES]
    index_files = {}
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
                file_blob_key = create_blob_key(file_info)
                file_string = handle.get(bucket_name, file_blob_key).decode("utf-8")
                file_json = json.loads(file_string)
            # TODO (mbaumann) Are there other JSON-related exceptions that should be checked below?
            except json.decoder.JSONDecodeError as ex:
                logger.warning(f"In bundle {bundle_id} the file \"{file_info[BundleFileMetadata.NAME]}\""
                               " is marked for indexing yet could not be parsed."
                               " This file will not be indexed. Exception: %s", ex)
                continue
            except BlobStoreError as ex:
                logger.warning(f"In bundle {bundle_id} the file \"{file_info[BundleFileMetadata.NAME]}\""
                               " is marked for indexing yet could not be accessed."
                               " This file will not be indexed. Exception: %s, File blob key: %s",
                               type(ex).__name__, file_blob_key)
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


def get_bundle_id_from_key(bundle_key: str) -> str:
    bundle_prefix = "bundles/"
    if bundle_key.startswith(bundle_prefix):
        return bundle_key[len(bundle_prefix):]
    raise Exception(f"This is not a key for a bundle: {bundle_key}")


def add_index_data_to_elasticsearch(bundle_id: str, index_data: dict, index_name: str, logger) -> None:
    create_elasticsearch_index(index_name, logger)
    logger.debug("Adding index data to Elasticsearch index '%s': %s", index_name, json.dumps(index_data, indent=4))
    add_data_to_elasticsearch(bundle_id, index_data, index_name, logger)


def create_elasticsearch_index(index_name, logger):
    path = os.path.join(os.path.dirname(__file__), "mapping.json")
    get_elasticsearch_index(ElasticsearchClient.get(logger), index_name, logger, path=path)


def add_data_to_elasticsearch(bundle_id: str, index_data: dict, index_name: str, logger) -> None:
    try:
        ElasticsearchClient.get(logger).index(index=index_name,
                                              doc_type=ESDocType.doc.name,
                                              id=bundle_id,
                                              body=json.dumps(index_data))  # Do not use refresh here - too expensive.
    except Exception as ex:
        logger.error("Document not indexed. Exception: %s, Index name: %s,  Index data: %s", ex, index_name,
                     json.dumps(index_data, indent=4))
        raise


def find_matching_subscriptions(index_data: dict, index_name: str, logger) -> set:
    percolate_document = {
        'query': {
            'percolate': {
                'field': "query",
                'document_type': ESDocType.doc.name,
                'document': index_data
            }
        }
    }
    subscription_ids = set()
    for hit in scan(ElasticsearchClient.get(logger),
                    index=index_name,
                    query=percolate_document):
        subscription_ids.add(hit["_id"])
    logger.debug("Found matching subscription count: %i", len(subscription_ids))
    return subscription_ids


def process_notifications(bundle_id: str, subscription_ids: set, replica, logger) -> None:
    for subscription_id in subscription_ids:
        try:
            # TODO Batch this request
            subscription = get_subscription(subscription_id, replica, logger)
            notify(subscription_id, subscription, bundle_id, logger)
        except Exception as ex:
            logger.error("Error occurred while processing subscription %s for bundle %s. %s",
                         subscription_id, bundle_id, ex)


def get_subscription(subscription_id: str, replica: str, logger):
    subscription_query = {
        'query': {
            'ids': {
                'type': ESDocType.subscription.name,
                'values': [subscription_id]
            }
        }
    }
    response = ElasticsearchClient.get(logger).search(
        index=Config.get_es_index_name(ESIndexType.subscriptions, Replica[replica]),
        body=subscription_query)
    if len(response['hits']['hits']) == 1:
        return response['hits']['hits'][0]['_source']


def notify(subscription_id: str, subscription: dict, bundle_id: str, logger):
    bundle_uuid, _, bundle_version = bundle_id.partition(".")
    transaction_id = str(uuid.uuid4())
    payload = {
        "transaction_id": transaction_id,
        "subscription_id": subscription_id,
        "es_query": subscription['es_query'],
        "match": {
            "bundle_uuid": bundle_uuid,
            "bundle_version": bundle_version
        }
    }
    callback_url = subscription['callback_url']

    # FIXME wrap all errors in this block with an exception handler
    if DeploymentStage.IS_PROD():
        allowed_schemes = {'https'}
    else:
        allowed_schemes = {'https', 'http'}

    assert urlparse(callback_url).scheme in allowed_schemes, "Unexpected scheme for callback URL"

    if DeploymentStage.IS_PROD():
        hostname = urlparse(callback_url).hostname
        for family, socktype, proto, canonname, sockaddr in socket.getaddrinfo(hostname, port=None):
            msg = "Callback hostname resolves to forbidden network"
            assert ipaddress.ip_address(sockaddr[0]).is_global, msg  # type: ignore

    auth = None
    if "hmac_secret_key" in subscription:
        auth = HTTPSignatureAuth(key=subscription['hmac_secret_key'].encode(),
                                 key_id=subscription.get("hmac_key_id", "hca-dss:" + subscription_id))
    response = requests.post(callback_url, json=payload, auth=auth)

    # TODO (mbaumann) Add webhook retry logic
    if 200 <= response.status_code < 300:
        logger.info(f"Successfully notified for subscription {subscription_id}"
                    f" for bundle {bundle_id} with transaction id {transaction_id} Code: {response.status_code}")
    else:
        logger.warning(f"Failed notification for subscription {subscription_id}"
                       f" for bundle {bundle_id} with transaction id {transaction_id} Code: {response.status_code}")
