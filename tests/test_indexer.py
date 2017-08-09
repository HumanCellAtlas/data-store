#!/usr/bin/env python
# coding: utf-8

import json
import logging
import os
import sys
import threading
import time
import unittest
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Dict

import boto3
import google.auth
import google.auth.transport.requests
import moto
import requests
from botocore.exceptions import ClientError

import dss
from dss import (DeploymentStage, Config,
                 DSS_ELASTICSEARCH_INDEX_NAME, DSS_ELASTICSEARCH_DOC_TYPE,
                 DSS_ELASTICSEARCH_SUBSCRIPTION_INDEX_NAME)
from dss.events.handlers.index import process_new_indexable_object
from dss.util import create_blob_key, UrlBuilder
from dss.util.es import ElasticsearchClient

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa
fixtures_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'fixtures'))  # noqa
sys.path.insert(0, fixtures_root)  # noqa


from tests.es import check_start_elasticsearch_service, elasticsearch_delete_index
from tests.fixtures.populate import populate
from tests.infra import DSSAsserts, StorageTestSupport, S3TestBundle, start_verbose_logging


# The moto mock has two defects that show up when used by the dss core storage system.
# Use actual S3 until these defects are fixed in moto.
# TODO (mbaumann) When the defects in moto have been fixed, remove True from the line below.
USE_AWS_S3 = bool(os.environ.get("USE_AWS_S3", True))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


start_verbose_logging()
for logger_name in logging.Logger.manager.loggerDict:  # type: ignore
    if logger_name.startswith("elasticsearch"):
        logging.getLogger(logger_name).setLevel(logging.WARNING)


#
# Basic test for DSS indexer:
#   1. Populate S3 bucket with data for a bundle as defined
#      in the HCA Storage System Disk Format specification
#   2. Inject a mock S3 event into function used by the indexing AWS Lambda
#   3. Read and process the bundle manifest to produce an index as
#      defined in HCA Storage System Index, Query, and Eventing Functional Spec & Use Cases
#      The index document is then added to Elasticsearch
#   4. Perform a search to verify the bundle index document is in Elasticsearch.
#   5. Verify the structure and content of the index document
#


class TestIndexer(unittest.TestCase, DSSAsserts, StorageTestSupport):

    http_server_address = "127.0.0.1"
    http_server_port = 8729

    @classmethod
    def setUpClass(cls):
        Config.set_config(DeploymentStage.TEST)
        if not USE_AWS_S3:  # Setup moto mocks
            cls.mock_s3 = moto.mock_s3()
            cls.mock_s3.start()
            cls.mock_sts = moto.mock_sts()
            cls.mock_sts.start()
            Config.set_config(DeploymentStage.TEST_FIXTURE)
            create_s3_bucket(Config.get_s3_bucket())
            populate(Config.get_s3_bucket(), None)
            Config.set_config(DeploymentStage.TEST)
            create_s3_bucket(Config.get_s3_bucket())

        if "DSS_ES_ENDPOINT" not in os.environ:
            os.environ["DSS_ES_ENDPOINT"] = "localhost"
        check_start_elasticsearch_service()

        cls.http_server = HTTPServer((cls.http_server_address, cls.http_server_port), PostTestHandler)
        cls.http_server_thread = threading.Thread(target=cls.http_server.serve_forever)
        cls.http_server_thread.start()

    @classmethod
    def tearDownClass(cls):
        if not USE_AWS_S3:  # Teardown moto mocks
            cls.mock_sts.stop()
            cls.mock_s3.stop()
        cls.http_server.shutdown()

    def setUp(self):
        self.app = dss.create_app().app.test_client()
        elasticsearch_delete_index("_all")
        PostTestHandler.reset()

    def tearDown(self):
        self.app = None
        self.storageHelper = None

    def test_process_new_indexable_object(self):
        bundle_key = self.load_test_data_bundle_for_path("fixtures/smartseq2/paired_ends")
        sample_s3_event = self.create_sample_s3_bundle_created_event(bundle_key)
        process_new_indexable_object(sample_s3_event, logger)
        search_results = self.get_search_results(smartseq2_paired_ends_query, 1)
        self.assertEqual(1, len(search_results))
        self.verify_index_document_structure_and_content(search_results[0], bundle_key,
                                                         files=smartseq2_paried_ends_indexed_file_list)

    def test_indexed_file_with_invalid_content_type(self):
        bundle = S3TestBundle("fixtures/smartseq2/paired_ends")
        # Configure a file to be indexed that is not of context type 'application/json'
        for file in bundle.files:
            if file.name == "text_data_file1.txt":
                file.indexed = True
        bundle_key = self.load_test_data_bundle(bundle)
        sample_s3_event = self.create_sample_s3_bundle_created_event(bundle_key)
        with self.assertLogs(logger, level="WARNING") as log_monitor:
            process_new_indexable_object(sample_s3_event, logger)
        self.assertRegex(log_monitor.output[0],
                         "WARNING:.*:In bundle .* the file \"text_data_file1.txt\" is marked for indexing"
                         " yet has content type \"text/plain\" instead of the required"
                         " content type \"application/json\". This file will not be indexed.")
        search_results = self.get_search_results(smartseq2_paired_ends_query, 1)
        self.assertEqual(1, len(search_results))
        self.verify_index_document_structure_and_content(search_results[0], bundle_key,
                                                         files=smartseq2_paried_ends_indexed_file_list)

    def test_indexed_file_unparsable(self):
        bundle_key = self.load_test_data_bundle_for_path("fixtures/unparseable_indexed_file")
        sample_s3_event = self.create_sample_s3_bundle_created_event(bundle_key)
        with self.assertLogs(logger, level="WARNING") as log_monitor:
            process_new_indexable_object(sample_s3_event, logger)
        self.assertRegex(log_monitor.output[0],
                         "WARNING:.*:In bundle .* the file \"unparseable_json.json\" is marked for indexing"
                         " yet could not be parsed. This file will not be indexed. Exception:")
        search_results = self.get_search_results(smartseq2_paired_ends_query, 1)
        self.assertEqual(1, len(search_results))
        self.verify_index_document_structure_and_content(search_results[0], bundle_key,
                                                         files=smartseq2_paried_ends_indexed_file_list)

    def test_indexed_file_access_error(self):
        bundle_key = self.load_test_data_bundle_for_path("fixtures/smartseq2/paired_ends")
        filename = "project.json"
        deleteFileBlob(bundle_key, filename)
        sample_s3_event = self.create_sample_s3_bundle_created_event(bundle_key)
        with self.assertLogs(logger, level="WARNING") as log_monitor:
            process_new_indexable_object(sample_s3_event, logger)
        self.assertRegex(log_monitor.output[0],
                         f"WARNING:.*:In bundle .* the file \"{filename}\" is marked for indexing"
                         " yet could not be accessed. This file will not be indexed. Exception:")
        search_results = self.get_search_results(smartseq2_paired_ends_query, 1)
        self.assertEqual(1, len(search_results))
        self.verify_index_document_structure_and_content(search_results[0], bundle_key,
                                                         files=smartseq2_paried_ends_indexed_file_list,
                                                         excluded_files=[filename.replace('.', '_')])

    def test_es_client_reuse(self):
        from dss.events.handlers.index import ElasticsearchClient
        bundle_key = self.load_test_data_bundle_for_path("fixtures/smartseq2/paired_ends")
        sample_s3_event = self.create_sample_s3_bundle_created_event(bundle_key)

        ElasticsearchClient._es_client = None
        process_new_indexable_object(sample_s3_event, logger)
        self.assertIsNotNone(ElasticsearchClient._es_client)
        es_client_after_first_call = ElasticsearchClient._es_client

        process_new_indexable_object(sample_s3_event, logger)
        self.assertIsNotNone(ElasticsearchClient._es_client)
        es_client_after_second_call = ElasticsearchClient._es_client

        self.assertIs(es_client_after_first_call, es_client_after_second_call)

    def test_subscription_notification_successful(self):
        bundle_key = self.load_test_data_bundle_for_path("fixtures/smartseq2/paired_ends")
        sample_s3_event = self.create_sample_s3_bundle_created_event(bundle_key)
        process_new_indexable_object(sample_s3_event, logger)

        ElasticsearchClient.get(logger).indices.create(DSS_ELASTICSEARCH_SUBSCRIPTION_INDEX_NAME)
        subscription_id = self.subscribe_for_notification(smartseq2_paired_ends_query,
                                                          f"http://{self.http_server_address}:{self.http_server_port}")

        bundle_key = self.load_test_data_bundle_for_path("fixtures/smartseq2/paired_ends")
        sample_s3_event = self.create_sample_s3_bundle_created_event(bundle_key)
        process_new_indexable_object(sample_s3_event, logger)
        prefix, _, bundle_id = bundle_key.partition("/")
        self.verify_notification(subscription_id, smartseq2_paired_ends_query, bundle_id)

    def test_subscription_notification_unsuccessful(self):
        bundle_key = self.load_test_data_bundle_for_path("fixtures/smartseq2/paired_ends")
        sample_s3_event = self.create_sample_s3_bundle_created_event(bundle_key)
        process_new_indexable_object(sample_s3_event, logger)

        ElasticsearchClient.get(logger).indices.create(DSS_ELASTICSEARCH_SUBSCRIPTION_INDEX_NAME)
        subscription_id = self.subscribe_for_notification(smartseq2_paired_ends_query,
                                                          f"http://{self.http_server_address}:{self.http_server_port}")

        bundle_key = self.load_test_data_bundle_for_path("fixtures/smartseq2/paired_ends")
        sample_s3_event = self.create_sample_s3_bundle_created_event(bundle_key)
        error_response_code = 500
        PostTestHandler.set_response_code(error_response_code)
        with self.assertLogs(logger, level="WARNING") as log_monitor:
            process_new_indexable_object(sample_s3_event, logger)
        prefix, _, bundle_id = bundle_key.partition("/")
        self.assertRegex(log_monitor.output[0],
                         f"WARNING:.*:Failed notification for subscription {subscription_id}"
                         f" for bundle {bundle_id} with transaction id .+ Code: {error_response_code}")

    def verify_notification(self, subscription_id, query, bundle_id):
        posted_payload_string = self.get_notification_payload()
        self.assertIsNotNone(posted_payload_string)
        posted_json = json.loads(posted_payload_string)
        self.assertIn('transaction_id', posted_json)
        self.assertIn('subscription_id', posted_json)
        self.assertEqual(subscription_id, posted_json['subscription_id'])
        self.assertIn('query', posted_json)
        self.assertEqual(query, posted_json['query'])
        self.assertIn('match', posted_json)
        bundle_uuid, _, bundle_version = bundle_id.partition(".")
        self.assertEqual(bundle_uuid, posted_json['match']['bundle_uuid'])
        self.assertEqual(bundle_version, posted_json['match']['bundle_version'])

    @staticmethod
    def get_notification_payload():
        timeout = 2
        timeout_time = time.time() + timeout
        while True:
            posted_payload_string = PostTestHandler.get_payload()
            if posted_payload_string:
                return posted_payload_string
            if time.time() >= timeout_time:
                return None
            else:
                time.sleep(0.5)

    def load_test_data_bundle_for_path(self, fixture_path: str):
        bundle = S3TestBundle(fixture_path)
        return self.load_test_data_bundle(bundle)

    def load_test_data_bundle(self, bundle: S3TestBundle):
        self.upload_files_and_create_bundle(bundle)
        return f"bundles/{bundle.uuid}.{bundle.version}"

    def subscribe_for_notification(self, query, callback_url):
        url = str(UrlBuilder()
                  .set(path="/v1/subscriptions")
                  .add_query("replica", "aws"))
        resp_obj = self.assertPutResponse(
            url,
            requests.codes.created,
            json_request_body=dict(
                query=query,
                callback_url=callback_url),
            headers=self.get_auth_header()
        )
        uuid_ = resp_obj.json['uuid']
        return uuid_

    def get_auth_header(self, token=None):
        credentials, project_id = google.auth.default(scopes=["https://www.googleapis.com/auth/userinfo.email"])

        if not token:
            r = google.auth.transport.requests.Request()
            credentials.refresh(r)
            r.session.close()
            token = credentials.token

        return {'Authorization': f"Bearer {token}"}

    @staticmethod
    def create_sample_s3_bundle_created_event(bundle_key: str) -> Dict:
        with open(os.path.join(os.path.dirname(__file__), "sample_s3_bundle_created_event.json")) as fh:
            sample_s3_event = json.load(fh)
        sample_s3_event['Records'][0]["s3"]['bucket']['name'] = Config.get_s3_bucket()
        sample_s3_event['Records'][0]["s3"]['object']['key'] = bundle_key
        return sample_s3_event

    def verify_index_document_structure_and_content(self, actual_index_document,
                                                    bundle_key, files, excluded_files=[]):
        self.verify_index_document_structure(actual_index_document, files, excluded_files)
        expected_index_document = generate_expected_index_document(Config.get_s3_bucket(), bundle_key)
        if expected_index_document != actual_index_document:
            logger.error(f"Expected index document: {json.dumps(expected_index_document, indent=4)}")
            logger.error(f"Actual index document: {json.dumps(actual_index_document, indent=4)}")
            self.assertDictEqual(expected_index_document, actual_index_document)

    def verify_index_document_structure(self, index_document, files, excluded_files):
        self.assertEqual(3, len(index_document.keys()))
        self.assertEqual("new", index_document['state'])
        self.assertIsNotNone(index_document['manifest'])
        self.assertIsNotNone(index_document['files'])
        self.assertEqual((len(files) - len(excluded_files)),
                         len(index_document['files'].keys()))
        for filename in files:
            if filename not in excluded_files:
                self.assertIsNotNone(index_document['files'][filename])

    @classmethod
    def get_search_results(cls, query, expected_hit_count):
        # Elasticsearch periodically refreshes the searchable data with newly added data.
        # By default, the refresh interval is one second.
        # https://www.elastic.co/guide/en/elasticsearch/reference/5.5/_modifying_your_data.html
        # Yet, sometimes one second is not quite enough given the churn in the local Elasticsearch instance.
        timeout = 2
        timeout_time = time.time() + timeout
        while True:
            response = ElasticsearchClient.get(logger).search(
                index=DSS_ELASTICSEARCH_INDEX_NAME,
                doc_type=DSS_ELASTICSEARCH_DOC_TYPE,
                body=json.dumps(query))
            if (len(response['hits']['hits']) >= expected_hit_count) \
                    or (time.time() >= timeout_time):
                return [hit['_source'] for hit in response['hits']['hits']]
            else:
                time.sleep(0.5)


class PostTestHandler(BaseHTTPRequestHandler):
    _response_code = 200
    _payload = None

    def do_POST(self):
        self.send_response(self._response_code)
        self.end_headers()
        length = int(self.headers['content-length'])
        if length:
            PostTestHandler._payload = self.rfile.read(length).decode("utf-8")

    @classmethod
    def reset(cls):
        cls._response_code = 200
        cls._payload = None

    @classmethod
    def set_response_code(cls, code: int):
        cls._response_code = code

    @classmethod
    def get_payload(cls):
        return cls._payload

smartseq2_paried_ends_indexed_file_list = ["assay_json", "cell_json", "manifest_json", "project_json", "sample_json"]


smartseq2_paired_ends_query = \
    {
        'query': {
            'bool': {
                'must': [{
                    'match': {
                        "files.sample_json.donor.species": "Homo sapiens"
                    }
                }, {
                    'match': {
                        "files.assay_json.single_cell.method": "Fluidigm C1"
                    }
                }, {
                    'match': {
                        "files.sample_json.ncbi_biosample": "SAMN04303778"
                    }
                }]
            }
        }
    }


def create_s3_bucket(bucket_name) -> None:
    conn = boto3.resource("s3")
    try:
        conn.create_bucket(Bucket=bucket_name)
    except ClientError as ex:
        if ex.response['Error']['Code'] != "BucketAlreadyOwnedByYou":
            logger.error(f"An unexpected error occured when creating test bucket: {bucket_name}")

def deleteFileBlob(bundle_key, filename):
    s3 = boto3.resource("s3")
    manifest = read_bundle_manifest(s3, Config.get_s3_bucket(), bundle_key)
    files = manifest['files']
    for file_info in files:
        if file_info['name'] == filename:
            file_blob_key = create_blob_key(file_info)
            s3.Object(Config.get_s3_bucket(), file_blob_key).delete()
            return
    raise Exception(f"The file {filename} was not found in the manifest for bundle {bundle_key}")


def generate_expected_index_document(bucket_name, bundle_key):
    s3 = boto3.resource("s3")
    manifest = read_bundle_manifest(s3, bucket_name, bundle_key)
    index_data = create_index_data(s3, bucket_name, manifest)
    return index_data


def read_bundle_manifest(s3, bucket_name, bundle_key):
    manifest_string = s3.Object(bucket_name, bundle_key).get()['Body'].read().decode("utf-8")
    manifest = json.loads(manifest_string, encoding="utf-8")
    return manifest


def create_index_data(s3, bucket_name, manifest):
    index = dict(state="new", manifest=manifest)
    files_info = manifest['files']
    index_files = {}
    bucket = s3.Bucket(bucket_name)
    for file_info in files_info:
        if file_info['indexed'] is True:
            try:
                file_key = create_blob_key(file_info)
                obj = bucket.Object(file_key)
                if obj.metadata['hca-dss-content-type'] != "application/json":
                    continue
                file_string = obj.get()['Body'].read().decode("utf-8")
                file_json = json.loads(file_string)
            except Exception:
                continue
            index_filename = file_info["name"].replace(".", "_")
            index_files[index_filename] = file_json
    index['files'] = index_files
    return index

if __name__ == "__main__":
    unittest.main()
