import json
import logging
import os
import re

import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection

HCA_ES_INDEX_NAME = "hca-metadata"
HCA_METADATA_DOC_TYPE = "hca"

DSS_BUNDLE_KEY_REGEX = "^bundles/[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-4[0-9A-Fa-f]{3}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}\.[0-9]+$"

#
# Lambda function for DSS indexing
#

def process_new_indexable_object(event, context) -> None:
    try:
        # Currently this function is only called for S3 creation events
        log = context.log
        validate_environment_variables("AWS_DEFAULT_REGION", "DSS_S3_TEST_BUCKET", "DSS_ES_ENDPOINT")

        key = event['Records'][0]["s3"]["object"]["key"]
        if is_bundle_to_index(key):
            log.info("Received S3 creation event for bundle which will be indexed: %s", key)
            bucket = boto3.resource("s3").Bucket(event['Records'][0]["s3"]["bucket"]["name"])
            debug_log_object_info(bucket, key, log)
            check_for_tombstone(bucket, key, log)
            conn = boto3.resource('s3', region_name=os.getenv("AWS_DEFAULT_REGION"))
            manifest = read_bundle_manifest(conn, bucket, key, log)
            index_data = create_index_data(conn, bucket, key, manifest, log)
            add_index_data_to_elasticsearch(os.getenv("DSS_ES_ENDPOINT"), key, index_data, log)
            log.debug("Finished index processing of S3 creation event for bundle: %s", key)
        else:
            log.debug("Not indexing S3 creation event for key: %s", key)
    except Exception as e:
        context.log.error("Exception occurred while processing S3 event: %s Event: %s", e, json.dumps(event, indent=4))


def is_bundle_to_index(key) -> bool:
    # Check for pattern /bundles/<bundle_uuid>.<timestamp>
    # Don't process notifications explicitly for the latest bundle, of the format /bundles/<bundle_uuid>
    # The versioned/timestamped name for this same bundle will get processed, and the fully qualified
    # name will be needed to remove index data later if the bundle is deleted.
    result = re.search(DSS_BUNDLE_KEY_REGEX, key)
    return result is not None


# TODO Future - Identified in the spec yet not needed for current prototype.
def check_for_tombstone(bucket, key, log):
    pass


def read_bundle_manifest(conn, bucket, bundle_key, log):
    manifest_string = conn.Object(os.getenv("DSS_S3_TEST_BUCKET"), bundle_key).get()['Body'].read().decode("utf-8")
    log.debug("Read bundle manifest from bucket %s with bundle key %s: %s", bucket.name, bundle_key, manifest_string)
    manifest = json.loads(manifest_string, encoding="utf-8")
    return manifest


def create_index_data(conn, bucket, bundle_key, manifest, log):
    index = {}
    index['state'] = 'new'
    index['manifest'] = manifest
    files_info = manifest['files']
    index_files = {}
    for filename in files_info.keys():
        if files_info[filename]['indexed'] == 'True':
            if not filename.endswith(".json"):
                log.warning("File %s is marked for indexing but is not of type JSON. It will not be indexed.")
                continue
            log.debug("Indexing file: %s", filename)
            file_info = files_info[filename]
            file_key = create_file_key(file_info)
            file_string = conn.Object(os.getenv("DSS_S3_TEST_BUCKET"), file_key).get()['Body'].read().decode("utf-8")
            file_json = json.loads(file_string)
            index_files[filename] = file_json
    index['files'] = index_files
    return index


def create_file_key(file_info) -> str:
    return "blobs/{}.{}.{}.{}".format(file_info['sha256'], file_info['sha1'], file_info['s3-etag'], file_info['crc32c'])


def add_index_data_to_elasticsearch(elasticsearchEndpoint, bundle_key, index_data, log) -> None:
    es_client = connect_elasticsearch(elasticsearchEndpoint, log)
    create_elasticsearch_index(es_client, log)
    log.debug("Adding index data to Elasticsearch: %s", json.dumps(index_data, indent=4))
    add_data_to_elasticsearch(es_client, bundle_key, index_data, log)


def connect_elasticsearch(elasticsearch_endpoint: str, log) -> Elasticsearch:
    log.debug('Connecting to the ES Endpoint: %s', elasticsearch_endpoint)
    try:
        if (elasticsearch_endpoint is None or elasticsearch_endpoint == ""):
            raise Exception("The Elasticsearch endpoint is null or empty. " +
                            "Set environment variable DSS_ES_ENDPOINT to the Elasticsearch endpoint.")

        if elasticsearch_endpoint.endswith(".amazonaws.com"):
            log.debug("Connecting to AWS Elasticsearch service with endpoint: %s", elasticsearch_endpoint)
            es_client = Elasticsearch(
                hosts=[{'host': elasticsearch_endpoint, 'port': 443}],
                use_ssl=True,
                verify_certs=True,
                connection_class=RequestsHttpConnection)
        else:
            log.debug("Connecting to local Elasticsearch service.")
            es_client = Elasticsearch()  # Connect to local Elasticsearch
        return es_client
    except Exception as ex:
        log.error("Unable to connect to Elasticsearch endpoint %s. Exception: %s", elasticsearch_endpoint, ex)
        raise ex


def create_elasticsearch_index(es_client, log):
    try:
        response = es_client.indices.exists(HCA_ES_INDEX_NAME)
        if response is False:
            log.debug("Creating new Elasticsearch index: %s", HCA_ES_INDEX_NAME)
            response = es_client.indices.create(HCA_ES_INDEX_NAME, body=None)
            log.debug("Index creation response: %s", json.dumps(response, indent=4))
        else:
            log.debug("Using existing Elasticsearch index: %s", HCA_ES_INDEX_NAME)
    except Exception as ex:
        log.critical("Unable to create index %s  Exception: %s", HCA_ES_INDEX_NAME, ex)


def add_data_to_elasticsearch(es_client, bundle_key, index_data, log) -> None:
    try:
        if bundle_key.startswith("bundle/"):
            bundle_key = bundle_key[7:]
        es_client.index(index=HCA_ES_INDEX_NAME,
                        doc_type=HCA_METADATA_DOC_TYPE,
                        id=bundle_key,
                        body=json.dumps(index_data, indent=4))

    except Exception as ex:
        log.error("Document not indexed. Exception: %s  Index data: %s", ex, json.dumps(index_data, indent=4))


def validate_environment_variables(*environment_variable_names):
    invalid_variables = set()
    for environment_variable_name in environment_variable_names:
        variable_value = os.getenv(environment_variable_name)
        if variable_value is None or variable_value == "":
            invalid_variables.add(environment_variable_name)
    if invalid_variables:
        raise Exception("These required environment variables are unset or empty: {}".format(invalid_variables))


def debug_log_object_info(bucket, key, log) -> None:
    if log.isEnabledFor(logging.DEBUG):
        obj = bucket.Object(key)
        log.debug("Received an event from S3, object head: %s", obj.get(Range='bytes=0-80')["Body"].read())
