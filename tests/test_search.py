#!/usr/bin/env python
# coding: utf-8

import datetime
import json
import logging
import os
import sys
import time
import unittest
import uuid

import requests

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss import DSS_ELASTICSEARCH_INDEX_NAME, DSS_ELASTICSEARCH_DOC_TYPE
from dss.events.handlers.index import create_elasticsearch_index
from dss.util.es import ElasticsearchServer, ElasticsearchClient
from tests.infra import DSSAsserts, start_verbose_logging
from tests.es import elasticsearch_delete_index
from tests import smartseq2_paired_ends_query, get_version

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

start_verbose_logging()


class TestSearch(unittest.TestCase, DSSAsserts):
    @classmethod
    def setUpClass(cls):
        cls.es_server = ElasticsearchServer()
        os.environ['DSS_ES_PORT'] = str(cls.es_server.port)
        dss.Config.set_config(dss.DeploymentStage.TEST)
        cls.app = dss.create_app().app.test_client()
        with open(os.path.join(os.path.dirname(__file__), "sample_index_doc.json"), "r") as fh:
            cls.index_document = json.load(fh)

    @classmethod
    def load_sample_document(cls, file_name):
        with open(os.path.join(os.path.dirname(__file__), file_name), "r") as fh:
            return json.load(fh)

    @classmethod
    def tearDownClass(cls):
        cls.es_server.shutdown()

    def setUp(self):
        elasticsearch_delete_index("_all")
        create_elasticsearch_index(logger)

    def test_search_post(self):
        bundle_uuid = str(uuid.uuid4())
        version = get_version()
        self.index_document['manifest']['version'] = version
        bundle_id = f"{bundle_uuid}.{version}"
        bundle_url = f"http://localhost/v1/bundles/{bundle_uuid}?version={version}"

        es_client = ElasticsearchClient.get(logger)
        es_client.index(index=DSS_ELASTICSEARCH_INDEX_NAME,
                        doc_type=DSS_ELASTICSEARCH_DOC_TYPE,
                        id=bundle_id,
                        body=self.index_document,
                        refresh=True)

        response = self.assertPostResponse(
            "/v1/search",
            json_request_body=smartseq2_paired_ends_query,
            expected_code=requests.codes.ok)
        search_response = response.json
        self.assertDictEqual(search_response['query'], smartseq2_paired_ends_query)
        self.assertEqual(len(search_response), 2)
        self.assertEqual(len(search_response['results']), 1)
        self.assertEqual(search_response['results'][0]['bundle_id'], bundle_id)
        self.assertEqual(search_response['results'][0]['bundle_url'], bundle_url)

    def test_search_returns_no_results_when_no_documents_indexed(self):
        self.verify_search_results(smartseq2_paired_ends_query)

    def test_search_returns_no_result_when_query_does_not_match_indexed_documents(self):
        query = \
            {
                "query": {
                    "match": {
                        "files.sample_json.donor.species": "xxx"
                    }
                }
            }

        self.populate_search_index(self.index_document, 1)
        self.verify_search_results(query)

    def test_search_returns_error_when_invalid_query_used(self):
        # Some types of invalid queries are detected by Elasticsearch DSL
        # and others by Elasticsearch itself, and the response codes differ.
        invalid_query_data = [
            (
                {
                    "query": {
                        "mtch": {
                            "SomethingInvalid": "xxx"
                        }
                    }
                },
                requests.codes.bad_request
            ),
            (
                {
                    "query": {
                        "match": ["SomethingInvalid", "xxx"]
                    }
                },
                requests.codes.internal_server_error
            )
        ]

        self.populate_search_index(self.index_document, 1)
        for query_data in invalid_query_data:
            with self.subTest("Invalid Queries"):
                self.assertPostResponse(
                    "/v1/search",
                    json_request_body=(query_data[0]),
                    expected_code=query_data[1])

    def test_search_returns_X_results_when_X_documents_match_query(self):
        test_matches = [0, 1, 9, 10, 11, 1000, 5000]
        bundle_ids = []
        indexed = 0
        for x in test_matches:
            create = x - indexed
            indexed = x
            bundle_ids.extend(self.populate_search_index(self.index_document, create))
            with self.subTest("Search Returns %i Matches When %i Documents Indexed.".format(x, x)):
                self.verify_search_results(smartseq2_paired_ends_query, x, bundle_ids)

    def test_elasticsearch_exception(self):
        # Test Elasticsearch exception handling by setting an invalid endpoint
        es_logger = logging.getLogger("elasticsearch")
        original_es_level = es_logger.getEffectiveLevel()
        original_es_endpoint = os.environ['DSS_ES_ENDPOINT']

        es_logger.setLevel("ERROR")
        try:
            os.environ['DSS_ES_ENDPOINT'] = "bogus"
            response = self.assertPostResponse(
                "/v1/search",
                json_request_body=smartseq2_paired_ends_query,
                expected_code=requests.codes.internal_server_error)
            self.assertEqual(response.json['code'], "elasticsearch_error")
            self.assertEqual(response.json['title'], "Elasticsearch operation failed")
        finally:
            os.environ['DSS_ES_ENDPOINT'] = original_es_endpoint
            es_logger.setLevel(original_es_level)

    def populate_search_index(self, index_document: dict, count: int) -> list:
        es_client = ElasticsearchClient.get(logger)
        bundles = []
        for i in range(count):
            bundle_uuid = str(uuid.uuid4())
            version = get_version()
            index_document['manifest']['version'] = version
            bundle_id = f"{bundle_uuid}.{version}"
            bundle_url = f"http://localhost/v1/bundles/{bundle_uuid}?version={version}"

            es_client.index(index=DSS_ELASTICSEARCH_INDEX_NAME,
                            doc_type=DSS_ELASTICSEARCH_DOC_TYPE,
                            id=bundle_id,
                            body=index_document)
            bundles.append((bundle_id, bundle_url))
        return bundles

    def verify_search_results(self, query, expected_result_length=0, bundles=[]):
        timeout = 5
        timeout_time = timeout + time.time()
        while time.time() <= timeout_time:
            response = self.assertPostResponse(
                "/v1/search",
                json_request_body=(query),
                expected_code=requests.codes.ok)
            search_response = response.json
            if len(search_response['results']) == expected_result_length:
                break
            elif len(search_response['results']) > expected_result_length:
                self.fail("elasticsearch more results than expected.")
            else:
                time.sleep(0.5)
        else:
            self.fail("elasticsearch failed to return all results.")

        self.assertDictEqual(search_response['query'], query)
        self.assertEqual(len(search_response), 2)
        self.assertEqual(len(search_response['results']), expected_result_length)
        result_bundles = [(hit['bundle_id'], hit['bundle_url'])
                          for hit in search_response['results']]
        for bundle in bundles:
            self.assertIn(bundle, result_bundles)
