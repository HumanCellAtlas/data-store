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
from elasticsearch import Elasticsearch

from dss.events.handlers.index import process_new_indexable_object
from tests import infra

fixtures_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'fixtures'))  # noqa
sys.path.insert(0, fixtures_root)  # noqa

from tests.fixtures.populate import populate
from tests.sample_data_loader import load_sample_data_bundle, create_s3_bucket

DSS_ELASTICSEARCH_INDEX_NAME = "hca"
DSS_ELASTICSEARCH_DOC_TYPE = "hca"

USE_AWS_S3_MOCK = os.environ.get("USE_AWS_S3_MOCK", True)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, pkg_root)

#
# Basic test for DSS indexer:
#   1. Populate S3 bucket (mock or real) with data for a bundle as defined
#      in the HCA Storage System Disk Format specification
#   2. Inject a mock S3 event into function used by the indexing AWS Lambda
#   3. Read and process the bundle manifest to produce an index as
#      defined in HCA Storage System Index, Query, and Eventing Functional Spec & Use Cases
#      The index document is then added to Elasticsearch
#   4. Perform as simple search to verify the index is in Elasticsearch.
#

def populate_moto_test_fixture_data():
    s3_bucket_test_fixtures = (infra.get_env("DSS_S3_BUCKET_TEST_FIXTURES"))
    create_s3_bucket(s3_bucket_test_fixtures)
    populate(s3_bucket_test_fixtures, None)

class TestEventHandlers(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if USE_AWS_S3_MOCK is True:
            cls.mock_s3 = moto.mock_s3()
            cls.mock_s3.start()
            populate_moto_test_fixture_data()

        if "DSS_ES_ENDPOINT" not in os.environ:
            os.environ["DSS_ES_ENDPOINT"] = "localhost"

        log.debug("Setting up Elasticsearch")
        check_start_elasticsearch_service()
        elasticsearch_delete_index("_all")

    @classmethod
    def tearDownClass(cls):
        if USE_AWS_S3_MOCK is True:
            cls.mock_s3.stop()

    def test_process_new_indexable_object(self):
        bundle_key = load_sample_data_bundle()
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
        self.verify_search_results(1)

    @staticmethod
    def create_sample_s3_bundle_created_event(bundle_key: str) -> Dict:
        with open(os.path.join(os.path.dirname(__file__), "sample_s3_bundle_created_event.json")) as fh:
            sample_s3_event = json.load(fh)
        sample_s3_event['Records'][0]["s3"]["bucket"]["name"] = infra.get_env("DSS_S3_BUCKET_TEST")
        sample_s3_event['Records'][0]["s3"]["object"]["key"] = bundle_key
        return sample_s3_event

    def verify_search_results(self, expected_hit_count):
        es_client = Elasticsearch()
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
        response = es_client.search(index=DSS_ELASTICSEARCH_INDEX_NAME, doc_type=DSS_ELASTICSEARCH_DOC_TYPE,
                                    body=json.dumps(query))
        self.assertEqual(expected_hit_count, response['hits']['total'])
        with open(os.path.join(os.path.dirname(__file__), "expected_index_document.json"), "r") as fh:
            expected_index_document = json.load(fh)
        actual_index_document = response['hits']['hits'][0]['_source']
        self.normalize_inherently_different_values_in_dict(expected_index_document, actual_index_document)
        expected_index_string = json.dumps(expected_index_document, indent=4)
        actual_index_string = json.dumps(actual_index_document, indent=4)
        if expected_index_string != actual_index_string:
            log.error(("Actual index returned from search is different than expected value. "
                       "Expected value: %s Actual value: %s"), expected_index_string, actual_index_string)
            # Uncomment the following to write the actual value to a file to faciltate comparison with the expected.
            # with open(os.path.join(os.path.dirname(__file__), "tmp_actual_index_document.json"), "w+") as fh:
            #    fh.write(actual_index_string)
            #    fh.write(os.linesep)
        self.assertEqual(expected_index_string, actual_index_string)
        close_elasticsearch_connections(es_client)

    def normalize_inherently_different_values_in_dict(self, expected_json_dict, actual_json_dict):
        keys_to_normalize = {'version', 'uuid'}
        for key in expected_json_dict.keys():
            expected_value = expected_json_dict[key]
            actual_value = actual_json_dict[key]
            if key in keys_to_normalize:
                actual_json_dict[key] = expected_json_dict[key]
            elif isinstance(expected_value, dict):
                self.normalize_inherently_different_values_in_dict(expected_value, actual_value)
            elif isinstance(expected_value, list):
                self.normalize_inherently_different_values_in_list(expected_value, actual_value)

    def normalize_inherently_different_values_in_list(self, expected_list, actual_list):
        for i in range(0, len(expected_list)):
            if isinstance(expected_list[i], dict):
                self.normalize_inherently_different_values_in_dict(expected_list[i], actual_list[i])


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
