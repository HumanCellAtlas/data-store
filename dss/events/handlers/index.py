import json
import os
import re
from urllib.parse import unquote

import boto3

from ... import DSS_ELASTICSEARCH_INDEX_NAME, DSS_ELASTICSEARCH_DOC_TYPE
from ...util import connect_elasticsearch

DSS_BUNDLE_KEY_REGEX = r"^bundles/[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-4[0-9A-Fa-f]{3}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}\..+$"

#
# Lambda function for DSS indexing
#


def process_new_indexable_object(event, logger) -> None:
    try:
        # This function is only called for S3 creation events
        key = unquote(event['Records'][0]["s3"]["object"]["key"])
        if is_bundle_to_index(key):
            logger.info("Received S3 creation event for bundle which will be indexed: %s", key)
            s3 = boto3.resource('s3')
            bucket_name = event['Records'][0]["s3"]["bucket"]["name"]
            manifest = read_bundle_manifest(s3, bucket_name, key, logger)
            index_data = create_index_data(s3, bucket_name, manifest, logger)
            add_index_data_to_elasticsearch(os.getenv("DSS_ES_ENDPOINT"), key, index_data, logger)
            logger.debug("Finished index processing of S3 creation event for bundle: %s", key)
        else:
            logger.debug("Not indexing S3 creation event for key: %s", key)
    except Exception as e:
        logger.error("Exception occurred while processing S3 event: %s Event: %s", e, json.dumps(event, indent=4))
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
    logger.debug("Read bundle manifest from bucket %s with bundle key %s: %s", bucket_name, bundle_key, manifest_string)
    manifest = json.loads(manifest_string, encoding="utf-8")
    return manifest


def create_index_data(s3, bucket_name, manifest, logger):
    index = dict(state="new", manifest=manifest)
    files_info = manifest['files']
    index_files = {}
    bucket = s3.Bucket(bucket_name)
    for file_info in files_info:
        if file_info['indexed'] is True:
            if not file_info["name"].endswith(".json"):  # TODO Don't check filename, if file is 'indexed' load json
                logger.warning("File %s is marked for indexing but is not of type JSON. It will not be indexed.",
                               file_info["name"])
                continue
            logger.debug("Indexing file: %s", file_info["name"])
            file_key = create_file_key(file_info)
            file_string = bucket.Object(file_key).get()['Body'].read().decode("utf-8")
            file_json = json.loads(file_string)
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
            index_filename = file_info["name"].replace(".", "_")
            index_files[index_filename] = file_json
    index['files'] = index_files
    return index


def create_file_key(file_info) -> str:
    return "blobs/{}.{}.{}.{}".format(file_info['sha256'], file_info['sha1'], file_info['s3-etag'], file_info['crc32c'])


def add_index_data_to_elasticsearch(elasticsearch_endpoint, bundle_key, index_data, logger) -> None:
    es_client = connect_elasticsearch(elasticsearch_endpoint, logger)
    create_elasticsearch_index(es_client, logger)
    logger.debug("Adding index data to Elasticsearch: %s", json.dumps(index_data, indent=4))
    add_data_to_elasticsearch(es_client, bundle_key, index_data, logger)


def create_elasticsearch_index(es_client, logger):
    try:
        response = es_client.indices.exists(DSS_ELASTICSEARCH_INDEX_NAME)
        if response is False:
            logger.debug("Creating new Elasticsearch index: %s", DSS_ELASTICSEARCH_INDEX_NAME)
            response = es_client.indices.create(DSS_ELASTICSEARCH_INDEX_NAME, body=None)
            logger.debug("Index creation response: %s", json.dumps(response, indent=4))
        else:
            logger.debug("Using existing Elasticsearch index: %s", DSS_ELASTICSEARCH_INDEX_NAME)
    except Exception as ex:
        logger.critical("Unable to create index %s  Exception: %s", DSS_ELASTICSEARCH_INDEX_NAME, ex)


def add_data_to_elasticsearch(es_client, bundle_key, index_data, logger) -> None:
    try:
        if bundle_key.startswith("bundles/"):
            bundle_key = bundle_key[8:]
        es_client.index(index=DSS_ELASTICSEARCH_INDEX_NAME,
                        doc_type=DSS_ELASTICSEARCH_DOC_TYPE,
                        id=bundle_key,
                        body=json.dumps(index_data, indent=4))

    except Exception as ex:
        logger.error("Document not indexed. Exception: %s  Index data: %s", ex, json.dumps(index_data, indent=4))
