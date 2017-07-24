import json
import os
import re
from urllib.parse import unquote

import boto3
import botocore

from ... import DSS_ELASTICSEARCH_INDEX_NAME, DSS_ELASTICSEARCH_DOC_TYPE
from ...hcablobstore import BundleMetadata, BundleFileMetadata
from ...util import connect_elasticsearch

DSS_BUNDLE_KEY_REGEX = r"^bundles/[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-4[0-9A-Fa-f]{3}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}\..+$"

"""
Lambda function for DSS indexing
"""

class ElasticsearchClient:
    _es_client = None

    @staticmethod
    def get(logger):
        if ElasticsearchClient._es_client is None:
            ElasticsearchClient._es_client = connect_elasticsearch(os.getenv("DSS_ES_ENDPOINT"), logger)
        return ElasticsearchClient._es_client


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
            logger.debug(f"Finished index processing of S3 creation event for bundle: {key}")
        else:
            logger.debug(f"Not indexing S3 creation event for key: {key}")
    except Exception as e:
        logger.error(f"Exception occurred while processing S3 event: {e} Event: {json.dumps(event, indent=4)}")
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
                logger.warning((f"In bundle {bundle_id} the file \"{file_info[BundleFileMetadata.NAME]}\""
                                " is marked for indexing yet has content type"
                                f" \"{file_info[BundleFileMetadata.CONTENT_TYPE]}\""
                                " instead of the required content type \"application/json\"."
                                " This file will not be indexed.")
                               )
                continue
            try:
                file_key = create_file_key(file_info)
                file_string = bucket.Object(file_key).get()['Body'].read().decode("utf-8")
                file_json = json.loads(file_string)
            # TODO (mbaumann) Are there other JSON-related exceptions that should be checked below?
            except json.decoder.JSONDecodeError as e:
                logger.warning((f"In bundle {bundle_id} the file \"{file_info[BundleFileMetadata.NAME]}\""
                                " is marked for indexing yet could not be parsed."
                                f" This file will not be indexed. Exception: {e}"))
                continue
            except botocore.exceptions.ClientError as e:
                logger.warning((f"In bundle {bundle_id} the file \"{file_info[BundleFileMetadata.NAME]}\""
                                " is marked for indexing yet could not be accessed."
                                f" This file will not be indexed. Exception: {e}"))
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
    if bundle_key.startswith("bundles/"):
        bundle_key = bundle_key[8:]
    return bundle_key


def create_file_key(file_info) -> str:
    return "blobs/" + ".".join((
        file_info[BundleFileMetadata.SHA256],
        file_info[BundleFileMetadata.SHA1],
        file_info[BundleFileMetadata.S3_ETAG],
        file_info[BundleFileMetadata.CRC32C]
    ))


def add_index_data_to_elasticsearch(bundle_key, index_data, logger) -> None:
    create_elasticsearch_index(logger)
    logger.debug(f"Adding index data to Elasticsearch: {json.dumps(index_data, indent=4)}")
    add_data_to_elasticsearch(bundle_key, index_data, logger)


def create_elasticsearch_index(logger):
    try:
        es_client = ElasticsearchClient.get(logger)
        response = es_client.indices.exists(DSS_ELASTICSEARCH_INDEX_NAME)
        if response is False:
            logger.debug(f"Creating new Elasticsearch index: {DSS_ELASTICSEARCH_INDEX_NAME}")
            response = es_client.indices.create(DSS_ELASTICSEARCH_INDEX_NAME, body=None)
            logger.debug(f"Index creation response: {json.dumps(response, indent=4)}")
        else:
            logger.debug(f"Using existing Elasticsearch index: {DSS_ELASTICSEARCH_INDEX_NAME}", )
    except Exception as ex:
        logger.critical(f"Unable to create index {DSS_ELASTICSEARCH_INDEX_NAME}  Exception: {ex}")


def add_data_to_elasticsearch(bundle_id, index_data, logger) -> None:
    try:
        ElasticsearchClient.get(logger).index(index=DSS_ELASTICSEARCH_INDEX_NAME,
                                              doc_type=DSS_ELASTICSEARCH_DOC_TYPE,
                                              id=bundle_id,
                                              body=json.dumps(index_data, indent=4))

    except Exception as ex:
        logger.error(f"Document not indexed. Exception: {ex}  Index data: {json.dumps(index_data, indent=4)}")
