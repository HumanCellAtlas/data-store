#!/usr/bin/env python
# coding: utf-8

import datetime
import io
import json
import logging
import os
import socket
import sys
import threading
import time
import unittest
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from random import randint
from typing import Dict

import google.auth
import google.auth.transport.requests
import moto
import requests

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss import Config, DeploymentStage
from dss.config import IndexSuffix
from dss.events.handlers.index import process_new_s3_indexable_object, process_new_gs_indexable_object
from dss.hcablobstore import BundleMetadata, BundleFileMetadata, FileMetadata
from dss.util import create_blob_key, UrlBuilder
from dss.util.es import ElasticsearchClient, ElasticsearchServer
from tests import get_version
from tests.es import elasticsearch_delete_index
from tests.fixtures.populate import populate
from tests.infra import DSSAsserts, DSSUploadMixin, StorageTestSupport, TestBundle, start_verbose_logging
from tests.sample_search_queries import smartseq2_paired_ends_query

# The moto mock has two defects that show up when used by the dss core storage system.
# Use actual S3 until these defects are fixed in moto.
# TODO (mbaumann) When the defects in moto have been fixed, remove True from the line below.
USE_AWS_S3 = bool(os.environ.get("USE_AWS_S3", True))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


start_verbose_logging()


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

class HTTPInfo:
    address = "127.0.0.1"
    port = None
    server = None
    thread = None

class ESInfo:
    server = None

def setUpModule():
    IndexSuffix.name = __name__.split('.')[-1]
    HTTPInfo.port = findOpenPort()
    HTTPInfo.server = HTTPServer((HTTPInfo.address, HTTPInfo.port), PostTestHandler)
    HTTPInfo.thread = threading.Thread(target=HTTPInfo.server.serve_forever)
    HTTPInfo.thread.start()

    ESInfo.server = ElasticsearchServer()
    os.environ['DSS_ES_PORT'] = str(ESInfo.server.port)

def tearDownModule():
    ESInfo.server.shutdown()
    HTTPInfo.server.shutdown()
    os.unsetenv('DSS_ES_PORT')


class TestIndexerBase(DSSAsserts, StorageTestSupport, DSSUploadMixin):

    @classmethod
    def indexer_setup(cls, replica):
        cls.replica = replica
        Config.set_config(DeploymentStage.TEST_FIXTURE)
        cls.blobstore, _, cls.test_fixture_bucket = Config.get_cloud_specific_handles(cls.replica)
        Config.set_config(DeploymentStage.TEST)
        _, _, cls.test_bucket = Config.get_cloud_specific_handles(cls.replica)
        cls.dss_index_name = dss.Config.get_es_index_name(dss.ESIndexType.docs, dss.Replica[cls.replica])
        cls.subscription_index_name = dss.Config.get_es_index_name(dss.ESIndexType.subscriptions,
                                                                   dss.Replica[cls.replica])

    def setUp(self):
        self.app = dss.create_app().app.test_client()
        elasticsearch_delete_index(f"*{IndexSuffix.name}")
        PostTestHandler.reset()

    def tearDown(self):
        self.app = None
        self.storageHelper = None

    def test_process_new_indexable_object(self):
        bundle_key = self.load_test_data_bundle_for_path("fixtures/smartseq2/paired_ends")
        sample_event = self.create_sample_bundle_created_event(bundle_key)
        self.process_new_indexable_object(sample_event, logger)
        search_results = self.get_search_results(smartseq2_paired_ends_query, 1)
        self.assertEqual(1, len(search_results))
        self.verify_index_document_structure_and_content(search_results[0], bundle_key,
                                                         files=smartseq2_paried_ends_indexed_file_list)

    def test_indexed_file_with_invalid_content_type(self):
        bundle = TestBundle(self.blobstore, "fixtures/smartseq2/paired_ends", self.test_fixture_bucket, self.replica)
        # Configure a file to be indexed that is not of context type 'application/json'
        for file in bundle.files:
            if file.name == "text_data_file1.txt":
                file.indexed = True
        bundle_key = self.load_test_data_bundle(bundle)
        sample_event = self.create_sample_bundle_created_event(bundle_key)
        with self.assertLogs(logger, level="WARNING") as log_monitor:
            self.process_new_indexable_object(sample_event, logger)
        self.assertRegex(log_monitor.output[0],
                         "WARNING:.*:In bundle .* the file \"text_data_file1.txt\" is marked for indexing"
                         " yet has content type \"text/plain\" instead of the required"
                         " content type \"application/json\". This file will not be indexed.")
        search_results = self.get_search_results(smartseq2_paired_ends_query, 1)
        self.assertEqual(1, len(search_results))
        self.verify_index_document_structure_and_content(search_results[0], bundle_key,
                                                         files=smartseq2_paried_ends_indexed_file_list)

    def test_key_is_not_indexed_when_processing_an_event_with_a_nonbundle_key(self):
        bundle_uuid = "{}.{}".format(str(uuid.uuid4()), get_version())
        bundle_key = "files/" + bundle_uuid
        sample_event = self.create_sample_bundle_created_event(bundle_key)
        log_last = logger.getEffectiveLevel()
        logger.setLevel(logging.DEBUG)
        try:
            with self.assertLogs(logger, level="DEBUG") as log_monitor:
                self.process_new_indexable_object(sample_event, logger)
            self.assertRegex(log_monitor.output[0], "DEBUG:.*Not indexing .* creation event for key: .*")
            with self.assertRaises(Exception) as ex:
                ElasticsearchClient.get(logger).get(index=self.dss_index_name,
                                                    doc_type=dss.ESIndexType.docs,
                                                    id=bundle_uuid)
            self.assertEqual('index_not_found_exception', ex.exception.error)
        finally:
            logger.setLevel(log_last)

    def test_error_message_logged_when_invalid_bucket_in_event(self):
        bundle_key = "bundles/{}.{}".format(str(uuid.uuid4()), get_version())
        sample_event = self.create_bundle_created_event(bundle_key, "fake")
        with self.assertLogs(logger, level="ERROR") as log_monitor:
            with self.assertRaises(Exception):
                self.process_new_indexable_object(sample_event, logger)
        self.assertRegex(log_monitor.output[0], "ERROR:.*Exception occurred while processing .* event:.*")

    def test_indexed_file_unparsable(self):
        bundle_key = self.load_test_data_bundle_for_path("fixtures/unparseable_indexed_file")
        sample_event = self.create_sample_bundle_created_event(bundle_key)
        with self.assertLogs(logger, level="WARNING") as log_monitor:
            self.process_new_indexable_object(sample_event, logger)
        self.assertRegex(log_monitor.output[0],
                         "WARNING:.*:In bundle .* the file \"unparseable_json.json\" is marked for indexing"
                         " yet could not be parsed. This file will not be indexed. Exception:")
        search_results = self.get_search_results(smartseq2_paired_ends_query, 1)
        self.assertEqual(1, len(search_results))
        self.verify_index_document_structure_and_content(search_results[0], bundle_key,
                                                         files=smartseq2_paried_ends_indexed_file_list)

    def test_indexed_file_access_error(self):
        inaccesssible_filename = "inaccessible_file.json"
        bundle_key = self.load_test_data_bundle_with_inaccessible_file(
            "fixtures/smartseq2/paired_ends", inaccesssible_filename, "application/json", True)
        sample_event = self.create_sample_bundle_created_event(bundle_key)
        with self.assertLogs(logger, level="WARNING") as log_monitor:
            self.process_new_indexable_object(sample_event, logger)
        self.assertRegex(log_monitor.output[0],
                         f"WARNING:.*:In bundle .* the file \"{inaccesssible_filename}\" is marked for indexing"
                         " yet could not be accessed. This file will not be indexed. Exception: .*, File blob key:")
        search_results = self.get_search_results(smartseq2_paired_ends_query, 1)
        self.assertEqual(1, len(search_results))
        files = list(smartseq2_paried_ends_indexed_file_list)
        files.append(inaccesssible_filename.replace(".", "_"))
        self.verify_index_document_structure_and_content(search_results[0], bundle_key,
                                                         files=files,
                                                         excluded_files=[inaccesssible_filename.replace(".", "_")])

    def test_subscription_notification_successful(self):
        bundle_key = self.load_test_data_bundle_for_path("fixtures/smartseq2/paired_ends")
        sample_event = self.create_sample_bundle_created_event(bundle_key)
        self.process_new_indexable_object(sample_event, logger)

        ElasticsearchClient.get(logger).indices.create(self.subscription_index_name)
        subscription_id = self.subscribe_for_notification(smartseq2_paired_ends_query,
                                                          f"http://{HTTPInfo.address}:{HTTPInfo.port}")

        bundle_key = self.load_test_data_bundle_for_path("fixtures/smartseq2/paired_ends")
        sample_event = self.create_sample_bundle_created_event(bundle_key)
        self.process_new_indexable_object(sample_event, logger)
        prefix, _, bundle_id = bundle_key.partition("/")
        self.verify_notification(subscription_id, smartseq2_paired_ends_query, bundle_id)

    def test_subscription_notification_unsuccessful(self):
        bundle_key = self.load_test_data_bundle_for_path("fixtures/smartseq2/paired_ends")
        sample_event = self.create_sample_bundle_created_event(bundle_key)
        self.process_new_indexable_object(sample_event, logger)

        ElasticsearchClient.get(logger).indices.create(self.subscription_index_name)
        subscription_id = self.subscribe_for_notification(smartseq2_paired_ends_query,
                                                          f"http://{HTTPInfo.address}:{HTTPInfo.port}")

        bundle_key = self.load_test_data_bundle_for_path("fixtures/smartseq2/paired_ends")
        sample_event = self.create_sample_bundle_created_event(bundle_key)
        error_response_code = 500
        PostTestHandler.set_response_code(error_response_code)
        with self.assertLogs(logger, level="WARNING") as log_monitor:
            self.process_new_indexable_object(sample_event, logger)
        prefix, _, bundle_id = bundle_key.partition("/")
        self.assertRegex(log_monitor.output[0],
                         f"WARNING:.*:Failed notification for subscription {subscription_id}"
                         f" for bundle {bundle_id} with transaction id .+ Code: {error_response_code}")

    def verify_notification(self, subscription_id, es_query, bundle_id):
        posted_payload_string = self.get_notification_payload()
        self.assertIsNotNone(posted_payload_string)
        posted_json = json.loads(posted_payload_string)
        self.assertIn('transaction_id', posted_json)
        self.assertIn('subscription_id', posted_json)
        self.assertEqual(subscription_id, posted_json['subscription_id'])
        self.assertIn('es_query', posted_json)
        self.assertEqual(es_query, posted_json['es_query'])
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
        """Loads files into test bucket and returns bundle id"""
        bundle = TestBundle(self.blobstore, fixture_path, self.test_fixture_bucket, self.replica)
        return self.load_test_data_bundle(bundle)

    def load_test_data_bundle_with_inaccessible_file(self, fixture_path: str,
                                                     inaccessible_filename: str,
                                                     inaccessible_file_content_type: str,
                                                     inaccessible_file_indexed: bool):
        bundle = TestBundle(self.blobstore, fixture_path, self.test_fixture_bucket, self.replica)
        self.load_test_data_bundle(bundle)
        bundle_builder = BundleBuilder(self.replica)
        for file in bundle.files:
            bundle_builder.add_file(self.test_bucket, file.name, file.indexed, f'{file.uuid}.{file.version}')
        bundle_builder.add_invalid_file(inaccessible_filename,
                                        inaccessible_file_content_type,
                                        inaccessible_file_indexed)
        bundle_builder.store(self.test_bucket)
        return 'bundles/' + bundle_builder.get_bundle_id()

    def load_test_data_bundle(self, bundle: TestBundle):
        self.upload_files_and_create_bundle(bundle, self.replica)
        return f"bundles/{bundle.uuid}.{bundle.version}"

    def subscribe_for_notification(self, es_query, callback_url):
        url = str(UrlBuilder()
                  .set(path="/v1/subscriptions")
                  .add_query("replica", self.replica))
        resp_obj = self.assertPutResponse(
            url,
            requests.codes.created,
            json_request_body=dict(
                es_query=es_query,
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

    def verify_index_document_structure_and_content(self, actual_index_document,
                                                    bundle_key, files, excluded_files=[]):
        self.verify_index_document_structure(actual_index_document, files, excluded_files)
        expected_index_document = generate_expected_index_document(self.blobstore, self.test_bucket, bundle_key)
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
                index=cls.dss_index_name,
                doc_type=dss.ESDocType.doc.name,
                body=json.dumps(query))
            if (len(response['hits']['hits']) >= expected_hit_count) \
                    or (time.time() >= timeout_time):
                return [hit['_source'] for hit in response['hits']['hits']]
            else:
                time.sleep(0.5)

    def create_sample_bundle_created_event(self, bundle_key):
        return self.create_bundle_created_event(bundle_key, self.test_bucket)

    def create_bundle_created_event(self, bundle_key, bucket_name):
        raise NotImplemented()

    def process_new_indexable_object(self, event, logger):
        raise NotImplemented()


class TestAWSIndexer(TestIndexerBase, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super().indexer_setup("aws")

    def process_new_indexable_object(self, event, logger):
        process_new_s3_indexable_object(event, logger)

    def create_bundle_created_event(self, bundle_key, bucket_name) -> Dict:
        with open(os.path.join(os.path.dirname(__file__), "sample_s3_bundle_created_event.json")) as fh:
            sample_event = json.load(fh)
        sample_event['Records'][0]["s3"]['bucket']['name'] = bucket_name
        sample_event['Records'][0]["s3"]['object']['key'] = bundle_key
        return sample_event


class TestGCPIndexer(TestIndexerBase, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super().indexer_setup("gcp")

    def process_new_indexable_object(self, event, logger):
        process_new_gs_indexable_object(event, logger)

    def create_bundle_created_event(self, bundle_key, bucket_name) -> Dict:
        with open(os.path.join(os.path.dirname(__file__), "sample_gs_bundle_created_event.json")) as fh:
            sample_event = json.load(fh)
        sample_event["bucket"] = bucket_name
        sample_event["name"] = bundle_key
        return sample_event


class BundleBuilder:
    def __init__(self, replica, bundle_id=None, bundle_version=None):
        self.blobstore, _, _ = Config.get_cloud_specific_handles(replica)
        self.bundle_id = bundle_id if bundle_id else str(uuid.uuid4())
        self.bundle_version = bundle_version if bundle_version else self._get_version()
        self.bundle_manifest = {
            BundleMetadata.FORMAT: BundleMetadata.FILE_FORMAT_VERSION,
            BundleMetadata.VERSION: self.bundle_version,
            BundleMetadata.FILES: [],
            BundleMetadata.CREATOR_UID: "0"
        }

    def get_bundle_id(self):
        return f'{self.bundle_id}.{self.bundle_version}'

    def add_file(self, bucket_name, name, indexed, file_id):
        # Add the existing file to the bundle manifest
        file_manifest_string = self.blobstore.get(bucket_name, f"files/{file_id}").decode("utf-8")
        file_manifest = json.loads(file_manifest_string, encoding="utf-8")
        file_uuid, file_version = file_id.split(".", 1)
        bundle_file_manifest = {
            BundleFileMetadata.NAME: name,
            BundleFileMetadata.UUID: file_uuid,
            BundleFileMetadata.VERSION: file_version,
            BundleFileMetadata.CONTENT_TYPE: file_manifest[FileMetadata.CONTENT_TYPE],
            BundleFileMetadata.INDEXED: indexed,
            BundleFileMetadata.CRC32C: file_manifest[FileMetadata.CRC32C],
            BundleFileMetadata.S3_ETAG: file_manifest[FileMetadata.S3_ETAG],
            BundleFileMetadata.SHA1: file_manifest[FileMetadata.SHA1],
            BundleFileMetadata.SHA256: file_manifest[FileMetadata.SHA256],
        }
        self.bundle_manifest[BundleMetadata.FILES].append(bundle_file_manifest)

    def add_invalid_file(self, name, content_type, indexed):
        bundle_file_manifest = {
            BundleFileMetadata.NAME: name,
            BundleFileMetadata.UUID: str(uuid.uuid4()),
            BundleFileMetadata.VERSION: self._get_version(),
            BundleFileMetadata.CONTENT_TYPE: content_type,
            BundleFileMetadata.INDEXED: indexed,
            BundleFileMetadata.CRC32C: "0",
            BundleFileMetadata.S3_ETAG: "0",
            BundleFileMetadata.SHA1: "0",
            BundleFileMetadata.SHA256: "0",
        }
        self.bundle_manifest[BundleMetadata.FILES].append(bundle_file_manifest)

    def store(self, bucket_name):
        self.blobstore.upload_file_handle(bucket_name,
                                          'bundles/' + self.get_bundle_id(),
                                          io.BytesIO(json.dumps(self.bundle_manifest).encode("utf-8")))

    def _get_version(self):
        return datetime.datetime.utcnow().strftime("%Y-%m-%dT%H%M%S.%fZ")


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


def create_s3_bucket(bucket_name) -> None:
    import boto3
    from botocore.exceptions import ClientError
    conn = boto3.resource("s3")
    try:
        conn.create_bucket(Bucket=bucket_name)
    except ClientError as ex:
        if ex.response['Error']['Code'] != "BucketAlreadyOwnedByYou":
            logger.error(f"An unexpected error occured when creating test bucket: {bucket_name}")


def generate_expected_index_document(blobstore, bucket_name, bundle_key):
    manifest = read_bundle_manifest(blobstore, bucket_name, bundle_key)
    index_data = create_index_data(blobstore, bucket_name, manifest)
    return index_data


def read_bundle_manifest(blobstore, bucket_name, bundle_key):
    manifest_string = blobstore.get(bucket_name, bundle_key).decode("utf-8")
    manifest = json.loads(manifest_string, encoding="utf-8")
    return manifest


def create_index_data(blobstore, bucket_name, manifest):
    index = dict(state="new", manifest=manifest)
    files_info = manifest['files']
    index_files = {}
    for file_info in files_info:
        if file_info['indexed'] is True:
            try:
                file_key = create_blob_key(file_info)
                obj = blobstore.get_user_metadata(bucket_name, file_key)
                if obj['hca-dss-content-type'] != "application/json":
                    continue
                file_string = blobstore.get(bucket_name, file_key).decode("utf-8")
                file_json = json.loads(file_string)
            except Exception:
                continue
            index_filename = file_info["name"].replace(".", "_")
            index_files[index_filename] = file_json
    index['files'] = index_files
    return index


def findOpenPort() -> int:
    while True:
        port = randint(1024, 65535)
        try:
            sock = socket.create_connection((HTTPInfo.address, port), 1)
            sock.close()
            # there's something there already.
        except (ConnectionRefusedError, socket.timeout):
            # ok this is an open port.
            break
    return port

if __name__ == "__main__":
    unittest.main()
