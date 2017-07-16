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
from elasticsearch import Elasticsearch

import dss
from dss.events.handlers.index import process_new_indexable_object
from tests.infra import get_env, StorageTestSupport, S3TestBundle

DSS_ELASTICSEARCH_INDEX_NAME = "hca"
DSS_ELASTICSEARCH_DOC_TYPE = "hca"

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, pkg_root)

#
# Basic test for DSS indexer:
#   1. Populate S3 bucket with data for a bundle as defined
#      in the HCA Storage System Disk Format specification
#   2. Inject a mock S3 event into function used by the indexing AWS Lambda
#   3. Read and process the bundle manifest to produce an index as
#      defined in HCA Storage System Index, Query, and Eventing Functional Spec & Use Cases
#      The index document is then added to Elasticsearch
#   4. Perform a search to verify the bundle index document is in Elasticsearch.
#   5. Verify the structure and content of bundle the index document
#


class TestIndexer(unittest.TestCase, StorageTestSupport):

    @classmethod
    def setUpClass(cls):
        if "DSS_ES_ENDPOINT" not in os.environ:
            os.environ["DSS_ES_ENDPOINT"] = "localhost"

        log.debug("Setting up Elasticsearch")
        check_start_elasticsearch_service()
        elasticsearch_delete_index("_all")

    def setUp(self):
        StorageTestSupport.setup(self)
        dss.Config.set_config(dss.BucketStage.TEST)
        self.app = dss.create_app().app.test_client()

    def tearDown(self):
        self.app = None
        self.storageHelper = None

    def load_test_data_bundle(self, fixture_path: str):
        bundle = S3TestBundle(fixture_path)
        self.upload_files_and_create_bundle(bundle)
        return f"bundles/{bundle.uuid}.{bundle.version}"

    def test_process_new_indexable_object(self):
        bundle_key = self.load_test_data_bundle('fixtures/smartseq2/paired_ends')
        sample_s3_event = self.create_sample_s3_bundle_created_event(bundle_key)
        log.debug("Submitting s3 bundle created event: %s", json.dumps(sample_s3_event, indent=4))
        process_new_indexable_object(sample_s3_event, log)
        # It seems there is sometimes a slight delay between when a document
        # is added to Elasticsearch and when it starts showing-up in searches.
        # This is especially true if the index has just been deleted, recreated,
        # document added then seached - all in immediate succession.
        # Better to write a search test method that would retry the search until
        # the expected result was acheived or a timeout was reached.
        time.sleep(5)
        query = \
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
        search_results = self.get_search_results(query)
        self.assertEquals(1, len(search_results))
        self.verify_index_document_structure_and_content(search_results[0], bundle_key,
                                                         files=["assay_json", "cell_json", "manifest_json",
                                                                "project_json", "sample_json"])

    @staticmethod
    def create_sample_s3_bundle_created_event(bundle_key: str) -> Dict:
        with open(os.path.join(os.path.dirname(__file__), "sample_s3_bundle_created_event.json")) as fh:
            sample_s3_event = json.load(fh)
        sample_s3_event['Records'][0]["s3"]["bucket"]["name"] = get_env("DSS_S3_BUCKET_TEST")
        sample_s3_event['Records'][0]["s3"]["object"]["key"] = bundle_key
        return sample_s3_event

    def verify_index_document_structure_and_content(self, actual_index_document, bundle_key, files):
        self.verify_index_document_structure(actual_index_document, files)
        expected_index_document = generate_expected_index_document(get_env("DSS_S3_BUCKET_TEST"), bundle_key)
        if expected_index_document != actual_index_document:
            log.error("Expected index document: %s", json.dumps(expected_index_document, indent=4))
            log.error("Actual index document: %s", json.dumps(actual_index_document, indent=4))
            self.assertDictEqual(expected_index_document, actual_index_document)

    @staticmethod
    def get_search_results(query):
        es_client = Elasticsearch()
        try:
            response = es_client.search(index=DSS_ELASTICSEARCH_INDEX_NAME,
                                        doc_type=DSS_ELASTICSEARCH_DOC_TYPE,
                                        body=json.dumps(query))
            return [hit['_source'] for hit in response['hits']['hits']]

        finally:
            close_elasticsearch_connections(es_client)

    def verify_index_document_structure(self, index_document, files):
        self.assertEqual(3, len(index_document.keys()))
        self.assertEqual("new", index_document['state'])
        self.assertIsNotNone(index_document['manifest'])
        self.assertIsNotNone(index_document['files'])
        self.assertEqual(len(files), len(index_document['files'].keys()))
        for filename in files:
            self.assertIsNotNone(index_document['files'][filename])


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
            file_key = create_file_key(file_info)
            file_string = bucket.Object(file_key).get()['Body'].read().decode("utf-8")
            file_json = json.loads(file_string)
            index_filename = file_info["name"].replace(".", "_")
            index_files[index_filename] = file_json
    index['files'] = index_files
    return index


def create_file_key(file_info) -> str:
    return "blobs/{}.{}.{}.{}".format(file_info['sha256'], file_info['sha1'], file_info['s3-etag'],
                                      file_info['crc32c'])


# Check if the Elasticsearch service is running,
# and if not, raise and exception with instructions to start it.
def check_start_elasticsearch_service():
    try:
        es_client = Elasticsearch()
        es_info = es_client.info()
        log.debug("The Elasticsearch service is running.")
        log.debug("Elasticsearch info: %s", es_info)
        close_elasticsearch_connections(es_client)
    except Exception:
        raise Exception("The Elasticsearch service does not appear to be running on this system, "
                        "yet it is required for this test. Please start it by running: elasticsearch")


def elasticsearch_delete_index(index_name: str):
    try:
        es_client = Elasticsearch()
        es_client.indices.delete(index=index_name, ignore=[404])
        close_elasticsearch_connections(es_client)  # Prevents end-of-test complaints about open sockets
    except Exception as e:
        log.warning("Error occurred while removing Elasticsearch index:%s Exception: %s", index_name, e)


# This prevents open socket errors after the test is over.
def close_elasticsearch_connections(es_client):
    for conn in es_client.transport.connection_pool.connections:
        conn.pool.close()

if __name__ == '__main__':
    unittest.main()
