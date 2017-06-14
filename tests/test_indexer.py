#!/usr/bin/env python
# coding: utf-8

import json
import logging
import os
import subprocess
import sys
import time
import unittest
from logging import DEBUG
from typing import Dict

import moto
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

from dss.events.handlers.index import process_new_indexable_object  # noqa
from tests import utils
from tests.sample_data_loader import load_sample_data_bundle

HCA_ES_INDEX_NAME = "hca-metadata"
HCA_METADATA_DOC_TYPE = "hca"

USE_AWS_S3_MOCK = os.environ.get("USE_AWS_S3_MOCK", True)

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

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

class MockEventContext:
    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.log.setLevel(DEBUG)


class TestEventHandlers(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if USE_AWS_S3_MOCK is True:
            cls.mock_s3 = moto.mock_s3()
            cls.mock_s3.start()

        if "DSS_ES_ENDPOINT" not in os.environ:
            os.environ["DSS_ES_ENDPOINT"] = "localhost"

        log.debug("Setting-up Elasticsearch")
        check_start_elasticsearch_service()
        elasticsearch_delete_index("_all")

    @classmethod
    def tearDownClass(cls):
        if USE_AWS_S3_MOCK is True:
            cls.mock_s3.stop()

    def test_process_new_indexable_object(self):
        bundle_key = load_sample_data_bundle()
        sample_s3_event = self._create_sample_s3_bundle_created_event(bundle_key)
        log.debug("Submitting s3 bundle created event: %s", json.dumps(sample_s3_event, indent=4))
        process_new_indexable_object(sample_s3_event, MockEventContext())
        # It seems there is sometimes a slight delay between when a document
        # is added to Elasticsearch and when it starts showing-up in searches.
        # This is especially true if the index has just been deleted, recreated,
        # document added then seached - all in immediate succession.
        # Better to write a search test method that would retry the search until
        # the expected result was acheived or a timeout was reached.
        time.sleep(5)
        self._verify_search_results(1)

    def _create_sample_s3_bundle_created_event(self, bundle_key: str) -> Dict:
        with open(os.path.join(os.path.dirname(__file__), "sample_s3_bundle_created_event.json")) as fh:
            sample_s3_event = json.load(fh)
        sample_s3_event['Records'][0]["s3"]["bucket"]["name"] = utils.get_env("DSS_S3_TEST_BUCKET")
        sample_s3_event['Records'][0]["s3"]["object"]["key"] = bundle_key
        return sample_s3_event

    def _verify_search_results(self, expectedHitCount):
        es_client = Elasticsearch()
        s = Search(using=es_client, index=HCA_ES_INDEX_NAME, doc_type=HCA_METADATA_DOC_TYPE).query("match", state="new")
        response = s.execute()
        self.assertEqual(expectedHitCount, len(response.hits))
        for hit in response:
            print(hit.meta.score, hit)
        with open(os.path.join(os.path.dirname(__file__), "expected_index_document.json")) as fh:
            expected_index_document = json.load(fh)
        actual_index_document = response.hits.hits[0]['_source']
        self.normalize_values(expected_index_document, actual_index_document)
        self.assertEqual(json.dumps(expected_index_document, indent=4),
                         json.dumps(actual_index_document, indent=4))
        close_elasticsearch_connections(es_client)

    def normalize_values(self, expected_json_dict, actual_json_dict):
        keys_to_normalize = {'timestamp', 'uuid'}
        for key in expected_json_dict.keys():
            expected_value = expected_json_dict[key]
            actual_value = actual_json_dict[key]
            if key in keys_to_normalize:
                if  (len(expected_value) == len(actual_value)):
                    actual_json_dict[key] = expected_json_dict[key]
                else:
                    self.fail("The expected and actual values are different")
            elif type(expected_value) is type({}):
                self.normalize_values(expected_value, actual_value)

# Check if the Elasticsearch service is running,
# and if not, raise and exception with instructions to start it.
def check_start_elasticsearch_service():
    try:
        es_client = Elasticsearch()
        es_info = es_client.info()
        log.info("The Elasticsearch service is running.")
        log.debug("Elasticsearch info: %s", es_info)
        close_elasticsearch_connections(es_client)
    except Exception as e:
        raise Exception("The Elasticsearch service does not appear to be running on this system, "
                        "yet it is required for this test. Please start it by running: elasticsearch")

def elasticsearch_delete_index(index_name: str):
    try:
        es_client = Elasticsearch()
        es_client.indices.delete(index=index_name, ignore=[400, 404])
        close_elasticsearch_connections(es_client)  # Prevents end-of-test complaints about open sockets
    except Exception as e:
        log.warning("Error occurred while removing Elasticsearch index:%s Exception:%s", index_name, e)

# This prevents open socket errors after the test is over.
def close_elasticsearch_connections(es_client):
    for conn in es_client.transport.connection_pool.connections:
        conn.pool.close()

if __name__ == '__main__':
    unittest.main()
