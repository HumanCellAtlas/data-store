"""Lambda function for DSS indexing"""

import json
import logging
import os
import re
import uuid
import ipaddress
import socket
import typing
from urllib.parse import urlparse, unquote

import requests
from cloud_blobstore import BlobStore, BlobStoreError
from collections import defaultdict
from elasticsearch.helpers import scan, bulk, BulkIndexError
from requests_http_signature import HTTPSignatureAuth

from dss import Config, DeploymentStage, ESIndexType, ESDocType, Replica
from ...util import create_blob_key
from ...hcablobstore import BundleMetadata, BundleFileMetadata
from ...util.es import ElasticsearchClient, create_elasticsearch_doc_index

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
        document = BundleDocument.from_bucket(replica, bucket_name, key, logger)
        index_name = document.prepare_index()
        document.add_data_to_elasticsearch(index_name)
        document.notify_subscriptions(index_name)
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


class BundleDocument(dict):
    """
    An instance of this class represents the Elasticsearch document for a given bundle.
    """

    def __init__(self, replica: str, bundle_id: str, logger: logging.Logger) -> None:
        super().__init__()
        self.logger = logger
        self.replica = replica
        self.bundle_id = bundle_id

    @classmethod
    def from_bucket(cls, replica: str, bucket_name: str, key: str, logger):  # TODO: return type hint
        self = cls(replica, cls.get_bundle_id_from_key(key), logger)
        handle = Config.get_cloud_specific_handles(replica)[0]
        self['manifest'] = self._read_bundle_manifest(handle, bucket_name, key)
        self['files'] = self._read_file_infos(handle, bucket_name)
        self['state'] = 'new'
        return self

    @classmethod
    def from_json(cls, replica: str, bundle_id: str, bundle_json: dict, logger):  # TODO: return type hint
        self = cls(replica, bundle_id, logger)
        self.update(bundle_json)
        return self

    @property
    def files(self):
        return self['files']

    @property
    def manifest(self):
        return self['manifest']

    def _read_bundle_manifest(self, handle: BlobStore, bucket_name: str, bundle_key: str) -> dict:
        manifest_string = handle.get(bucket_name, bundle_key).decode("utf-8")
        self.logger.debug(f"Read bundle manifest from bucket {bucket_name}"
                          f" with bundle key {bundle_key}: {manifest_string}")
        manifest = json.loads(manifest_string, encoding="utf-8")
        return manifest

    def _read_file_infos(self, handle: BlobStore, bucket_name: str) -> dict:
        files_info = self.manifest[BundleMetadata.FILES]
        index_files = {}
        for file_info in files_info:
            if file_info[BundleFileMetadata.INDEXED] is True:
                if not file_info[BundleFileMetadata.CONTENT_TYPE].startswith('application/json'):
                    self.logger.warning(f"In bundle {self.bundle_id} the file '{file_info[BundleFileMetadata.NAME]}'"
                                        " is marked for indexing yet has content type"
                                        f" '{file_info[BundleFileMetadata.CONTENT_TYPE]}'"
                                        " instead of the required content type 'application/json'."
                                        " This file will not be indexed.")
                    continue
                file_blob_key = create_blob_key(file_info)
                try:
                    file_string = handle.get(bucket_name, file_blob_key).decode("utf-8")
                    file_json = json.loads(file_string)
                # TODO (mbaumann) Are there other JSON-related exceptions that should be checked below?
                except json.decoder.JSONDecodeError as ex:
                    self.logger.warning(f"In bundle {self.bundle_id} the file '{file_info[BundleFileMetadata.NAME]}'"
                                        " is marked for indexing yet could not be parsed."
                                        " This file will not be indexed. Exception: %s", ex)
                    continue
                except BlobStoreError as ex:
                    self.logger.warning(f"In bundle {self.bundle_id} the file '{file_info[BundleFileMetadata.NAME]}'"
                                        " is marked for indexing yet could not be accessed."
                                        " This file will not be indexed. Exception: %s, File blob key: %s",
                                        type(ex).__name__, file_blob_key)
                    continue
                self.logger.debug(f"Indexing file: {file_info[BundleFileMetadata.NAME]}")
                # There are two reasons in favor of not using dot in the name of the individual
                # files in the index document, and instead replacing it with an underscore.
                # 1. Ambiguity regarding interpretation/processing of dots in field names,
                #    which could potentially change between Elasticsearch versions. For example, see:
                #       https://github.com/elastic/elasticsearch/issues/15951
                # 2. The ES DSL queries are easier to read when there is no ambiguity regarding
                #    dot as a field separator.
                # Therefore, substitute dot for underscore in the key filename portion of the index.
                # As due diligence, additional investigation should be performed.
                index_filename = file_info[BundleFileMetadata.NAME].replace(".", "_")
                index_files[index_filename] = file_json
        return index_files

    def prepare_index(self):
        index_shape_identifier = self.get_index_shape_identifier()
        index_name = Config.get_es_index_name(ESIndexType.docs, Replica[self.replica], index_shape_identifier)
        create_elasticsearch_index(index_name, self.replica, self.logger)
        return index_name

    def get_index_shape_identifier(self) -> typing.Optional[str]:
        """ Return string identifying the shape/structure/format of the data in the index document,
        so that it may be indexed appropriately.

        Currently, this returns a string identifying the metadata schema release major number:
        For example:
            v3 - Bundle contains metadata in the version 3 format
            v4 - Bundle contains metadata in the version 4 format
            ...

        This includes verification that schema major number is the same for all index metadata
        files in the bundle, consistent with the current HCA ingest service behavior.
        If no metadata version information is contained in the bundle, the empty string is returned.
        Currently this occurs in the case of the empty bundle used for deployment testing.

        If/when bundle schemas are available, this function should be updated to reflect the
        bundle schema type and major version number.

        Other projects (non-HCA) may manage their metadata schemas (if any) and schema versions.
        This should be an extension point that is customizable by other projects according to their metadata.
        """

        schema_version_map = defaultdict(set)  # type: typing.MutableMapping[str, typing.MutableSet[str]]
        for filename, file_content in self.files.items():
            core = file_content.get('core')
            if core is not None:
                schema_type = core['type']
                schema_version = core['schema_version']
                schema_version_major = schema_version.split(".")[0]
                schema_version_map[schema_version_major].add(schema_type)
            else:
                self.logger.info("%s", (f"File {filename} does not contain a 'core' section to identify "
                                        "the schema and schema version."))
        if schema_version_map:
            schema_versions = schema_version_map.keys()
            assert len(schema_versions) == 1, \
                "The bundle contains mixed schema major version numbers: {}".format(sorted(list(schema_versions)))
            return "v" + list(schema_versions)[0]
        else:
            return None  # No files with schema identifiers were found

    @staticmethod
    def get_bundle_id_from_key(bundle_key: str) -> str:
        bundle_prefix = "bundles/"
        if bundle_key.startswith(bundle_prefix):
            return bundle_key[len(bundle_prefix):]
        raise Exception(f"This is not a key for a bundle: {bundle_key}")

    def add_data_to_elasticsearch(self, index_name: str) -> None:
        es_client = ElasticsearchClient.get(self.logger)
        try:
            self.logger.debug("Adding index data to Elasticsearch index '%s': %s", index_name,
                              json.dumps(self, indent=4))  # noqa
            initial_mappings = es_client.indices.get_mapping(index_name)[index_name]['mappings']
            es_client.index(index=index_name,
                            doc_type=ESDocType.doc.name,
                            id=self.bundle_id,
                            # FIXME: (hannes) Can this be json.dumps(self, indent=4) ?
                            body=json.dumps(self))  # Don't use refresh here.
        except Exception as ex:
            self.logger.error("Document not indexed. Exception: %s, Index name: %s,  Index data: %s",
                              ex, index_name, json.dumps(self, indent=4))
            raise

        try:
            current_mappings = es_client.indices.get_mapping(index_name)[index_name]['mappings']
            if initial_mappings != current_mappings:
                self.refresh_percolate_queries(index_name)
        except Exception as ex:
            self.logger.error("Error refreshing subscription queries for index. Exception: %s, Index name: %s",
                              ex, index_name)
            raise

    def refresh_percolate_queries(self, index_name: str) -> None:
        # When dynamic templates are used and queries for percolation have been added
        # to an index before the index contains mappings of fields referenced by those queries,
        # the queries must be reloaded when the mappings are present for the queries to match.
        # See: https://github.com/elastic/elasticsearch/issues/5750
        subscription_index_name = Config.get_es_index_name(ESIndexType.subscriptions, Replica[self.replica])
        es_client = ElasticsearchClient.get(self.logger)
        if not es_client.indices.exists(subscription_index_name):
            return
        subscription_queries = [{'_index': index_name,
                                 '_type': ESDocType.query.name,
                                 '_id': hit['_id'],
                                 '_source': hit['_source']['es_query']
                                 }
                                for hit in scan(es_client,
                                                index=subscription_index_name,
                                                doc_type=ESDocType.subscription.name,
                                                query={'query': {'match_all': {}}})
                                ]

        if subscription_queries:
            try:
                bulk(es_client, iter(subscription_queries), refresh=True)
            except BulkIndexError as ex:
                self.logger.error("Error occurred when adding subscription queries to index %s Errors: %s",
                                  index_name, ex.errors)

    def notify_subscriptions(self, index_name):
        subscriptions = self.find_matching_subscriptions(index_name)
        self.process_notifications(subscriptions)

    def find_matching_subscriptions(self, index_name: str) -> set:
        percolate_document = {
            'query': {
                'percolate': {
                    'field': "query",
                    'document_type': ESDocType.doc.name,
                    'document': self
                }
            }
        }
        subscription_ids = set()
        for hit in scan(ElasticsearchClient.get(self.logger),
                        index=index_name,
                        query=percolate_document):
            subscription_ids.add(hit["_id"])
        self.logger.debug("Found matching subscription count: %i", len(subscription_ids))
        return subscription_ids

    def process_notifications(self, subscription_ids: set) -> None:
        for subscription_id in subscription_ids:
            try:
                # TODO Batch this request
                subscription = self.get_subscription(subscription_id)
                self.notify(subscription_id, subscription)
            except Exception as ex:
                self.logger.error("Error occurred while processing subscription %s for bundle %s. %s",
                                  subscription_id, self.bundle_id, ex)

    def get_subscription(self, subscription_id: str):
        subscription_query = {
            'query': {
                'ids': {
                    'type': ESDocType.subscription.name,
                    'values': [subscription_id]
                }
            }
        }
        response = ElasticsearchClient.get(self.logger).search(
            index=Config.get_es_index_name(ESIndexType.subscriptions, Replica[self.replica]),
            body=subscription_query)
        if len(response['hits']['hits']) == 1:
            return response['hits']['hits'][0]['_source']

    def notify(self, subscription_id: str, subscription: dict):
        # FIXME: (hannes) brittle coupling with regex in is_bundle_to_index
        bundle_uuid, _, bundle_version = self.bundle_id.partition(".")
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
            self.logger.info(f"Successfully notified for subscription {subscription_id}"
                             f" for bundle {self.bundle_id} with transaction id {transaction_id} "
                             f"Code: {response.status_code}")
        else:
            self.logger.warning(f"Failed notification for subscription {subscription_id}"
                                f" for bundle {self.bundle_id} with transaction id {transaction_id} "
                                f"Code: {response.status_code}")


def create_elasticsearch_index(index_name: str, replica, logger: logging.Logger):
    es_client = ElasticsearchClient.get(logger)
    if not es_client.indices.exists(index_name):
        with open(os.path.join(os.path.dirname(__file__), "mapping.json"), "r") as fh:
            index_mapping = json.load(fh)
        index_mapping["mappings"][ESDocType.doc.name] = index_mapping["mappings"].pop("doc")
        index_mapping["mappings"][ESDocType.query.name] = index_mapping["mappings"].pop("query")
        alias_name = Config.get_es_alias_name(ESIndexType.docs, Replica[replica])
        create_elasticsearch_doc_index(es_client, index_name, alias_name, logger, index_mapping)
    else:
        logger.debug(f"Using existing Elasticsearch index: {index_name}")
