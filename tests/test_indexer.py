#!/usr/bin/env python
# coding: utf-8

import json
import logging
import os
import sys
import time
import unittest
from typing import Dict

import boto3
import moto
from botocore.exceptions import ClientError
from elasticsearch import Elasticsearch

import dss
from dss import BucketStage, Config, DSS_ELASTICSEARCH_INDEX_NAME, DSS_ELASTICSEARCH_DOC_TYPE
from dss.events.handlers.index import process_new_indexable_object, ElasticsearchClient
from dss.util import create_blob_key

fixtures_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'fixtures'))  # noqa
sys.path.insert(0, fixtures_root)  # noqa

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests.es import check_start_elasticsearch_service, elasticsearch_delete_index
from tests.fixtures.populate import populate
from tests.infra import DSSAsserts, StorageTestSupport, S3TestBundle

# The moto mock has two defects that show up when used by the dss core storage system.
# Use actual S3 until these defects are fixed in moto.
# TODO (mbaumann) When the defects in moto have been fixed, remove True from the line below.
USE_AWS_S3 = bool(os.environ.get("USE_AWS_S3", True))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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

    @classmethod
    def setUpClass(cls):
        if not USE_AWS_S3:  # Setup moto mocks
            cls.mock_s3 = moto.mock_s3()
            cls.mock_s3.start()
            cls.mock_sts = moto.mock_sts()
            cls.mock_sts.start()
            Config.set_config(BucketStage.TEST_FIXTURE)
            create_s3_bucket(Config.get_s3_bucket())
            populate(Config.get_s3_bucket(), None)
            Config.set_config(BucketStage.TEST)
            create_s3_bucket(Config.get_s3_bucket())

        if "DSS_ES_ENDPOINT" not in os.environ:
            os.environ["DSS_ES_ENDPOINT"] = "localhost"
        check_start_elasticsearch_service()

    @classmethod
    def tearDownClass(cls):
        if not USE_AWS_S3:  # Teardown moto mocks
            cls.mock_sts.stop()
            cls.mock_s3.stop()

    def setUp(self):
        self.app = dss.create_app().app.test_client()
        elasticsearch_delete_index("_all")

    def tearDown(self):
        self.app = None
        self.storageHelper = None

    def test_process_new_indexable_object(self):
        bundle_key = self.load_test_data_bundle_for_path('fixtures/smartseq2/paired_ends')
        sample_s3_event = self.create_sample_s3_bundle_created_event(bundle_key)
        process_new_indexable_object(sample_s3_event, logger)
        search_results = self.get_search_results(smartseq2_paired_ends_query, 1)
        self.assertEqual(1, len(search_results))
        self.verify_index_document_structure_and_content(search_results[0], bundle_key,
                                                         files=smartseq2_paried_ends_indexed_file_list)

    def test_indexed_file_with_invalid_content_type(self):
        bundle = S3TestBundle('fixtures/smartseq2/paired_ends')
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
        bundle_key = self.load_test_data_bundle_for_path('fixtures/unparseable_indexed_file')
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
        bundle_key = self.load_test_data_bundle_for_path('fixtures/smartseq2/paired_ends')
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
        bundle_key = self.load_test_data_bundle_for_path('fixtures/smartseq2/paired_ends')
        sample_s3_event = self.create_sample_s3_bundle_created_event(bundle_key)

        ElasticsearchClient._es_client = None
        process_new_indexable_object(sample_s3_event, logger)
        self.assertIsNotNone(ElasticsearchClient._es_client)
        es_client_after_first_call = ElasticsearchClient._es_client

        process_new_indexable_object(sample_s3_event, logger)
        self.assertIsNotNone(ElasticsearchClient._es_client)
        es_client_after_second_call = ElasticsearchClient._es_client

        self.assertIs(es_client_after_first_call, es_client_after_second_call)

    def load_test_data_bundle_for_path(self, fixture_path: str):
        bundle = S3TestBundle(fixture_path)
        return self.load_test_data_bundle(bundle)

    def load_test_data_bundle(self, bundle: S3TestBundle):
        self.upload_files_and_create_bundle(bundle)
        return f"bundles/{bundle.uuid}.{bundle.version}"

    @staticmethod
    def create_sample_s3_bundle_created_event(bundle_key: str) -> Dict:
        with open(os.path.join(os.path.dirname(__file__), "sample_s3_bundle_created_event.json")) as fh:
            sample_s3_event = json.load(fh)
        sample_s3_event['Records'][0]["s3"]["bucket"]["name"] = Config.get_s3_bucket()
        sample_s3_event['Records'][0]["s3"]["object"]["key"] = bundle_key
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


smartseq2_paried_ends_indexed_file_list = ["assay_json", "cell_json", "manifest_json", "project_json", "sample_json"]


smartseq2_paired_ends_query = \
    {
        "query": {
            "bool": {
                "must": [{
                    "match": {
                        "files.sample_json.donor.species": "Homo sapiens"
                    }
                }, {
                    "match": {
                        "files.assay_json.single_cell.method": "Fluidigm C1"
                    }
                }, {
                    "match": {
                        "files.sample_json.ncbi_biosample": "SAMN04303778"
                    }
                }]
            }
        }
    }


def create_s3_bucket(bucket_name) -> None:
    conn = boto3.resource('s3')
    try:
        conn.create_bucket(Bucket=bucket_name)
    except ClientError as ex:
        if ex.response['Error']['Code'] != 'BucketAlreadyOwnedByYou':
            logger.error(f"An unexpected error occured when creating test bucket: {bucket_name}")


def deleteFileBlob(bundle_key, filename):
    s3 = boto3.resource('s3')
    manifest = read_bundle_manifest(s3, Config.get_s3_bucket(), bundle_key)
    files = manifest['files']
    for file_info in files:
        if file_info['name'] == filename:
            file_blob_key = create_blob_key(file_info)
            s3.Object(Config.get_s3_bucket(), file_blob_key).delete()
            return
    raise Exception(f"The file {filename} was not found in the manifest for bundle {bundle_key}")


def deleteFileBlob(bundle_key, filename):
    s3 = boto3.resource('s3')
    manifest = read_bundle_manifest(s3, Config.get_s3_bucket(), bundle_key)
    files = manifest['files']
    for file_info in files:
        if file_info['name'] == filename:
            file_blob_key = create_file_key(file_info)
            s3.Object(Config.get_s3_bucket(), file_blob_key).delete()
            return
    raise Exception(f"The file {filename} was not found in the manifest for bundle {bundle_key}")


def generate_expected_index_document(bucket_name, bundle_key):
    s3 = boto3.resource('s3')
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
                obj = bucket.Object(file_key)
                if obj.metadata['hca-dss-content-type'] != 'application/json':
                    continue
                file_string = obj.get()['Body'].read().decode("utf-8")
                file_json = json.loads(file_string)
            except Exception:
                continue
            index_filename = file_info["name"].replace(".", "_")
            index_files[index_filename] = file_json
    index['files'] = index_files
    return index


if __name__ == '__main__':
    unittest.main()
