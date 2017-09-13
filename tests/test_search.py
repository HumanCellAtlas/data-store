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

from dss import ESDocType
from dss.util import UrlBuilder

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss.events.handlers.index import create_elasticsearch_index
from dss.util.es import ElasticsearchServer, ElasticsearchClient
from tests.infra import DSSAsserts, start_verbose_logging
from tests.es import elasticsearch_delete_index
from tests import get_version
from tests.sample_search_queries import smartseq2_paired_ends_query

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

start_verbose_logging()


class ESInfo:
    server = None


def setUpModule():
    ESInfo.server = ElasticsearchServer()
    os.environ['DSS_ES_PORT'] = str(ESInfo.server.port)


def tearDownModule():
    ESInfo.server.shutdown()


class TestSearchBase(DSSAsserts):
    @classmethod
    def search_setup(cls, replica):
        cls.replica_name = replica.name
        cls.dss_index_name = dss.Config.get_es_index_name(dss.ESIndexType.docs, replica)
        dss.Config.set_config(dss.DeploymentStage.TEST)
        cls.app = dss.create_app().app.test_client()
        with open(os.path.join(os.path.dirname(__file__), "sample_index_doc.json"), "r") as fh:
            cls.index_document = json.load(fh)

    def setUp(self):
        dss.Config.set_config(dss.DeploymentStage.TEST)
        self.app = dss.create_app().app.test_client()
        elasticsearch_delete_index(self.dss_index_name)
        create_elasticsearch_index(self.dss_index_name, logger)

    def test_search_post(self):
        bundle_uuid = str(uuid.uuid4())
        version = get_version()
        self.index_document['manifest']['version'] = version
        bundle_id = f"{bundle_uuid}.{version}"
        bundle_url = f"http://localhost/v1/bundles/{bundle_uuid}?version={version}"

        es_client = ElasticsearchClient.get(logger)
        es_client.index(index=self.dss_index_name,
                        doc_type=ESDocType.doc.name,
                        id=bundle_id,
                        body=self.index_document,
                        refresh=True)

        response = self.post_search(smartseq2_paired_ends_query, requests.codes.ok)
        search_response = response.json
        self.assertDictEqual(search_response['es_query'], smartseq2_paired_ends_query)
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
        for bad_query in invalid_query_data:
            with self.subTest("Invalid Queries: bad_query={bad_query[0]}"):
                self.post_search(*bad_query)

    def test_search_returns_N_results_when_N_documents_match_query(self):
        """Create N identical documents. A search query is executed to match the document. All documents created must be
        present in the query results. N is varied across a variety of values.
        """
        test_matches = [0, 1, 9, 10, 11, 1000, 5000]
        bundle_ids = []
        indexed = 0
        for x in test_matches:
            create = x - indexed
            indexed = x
            bundle_ids.extend(self.populate_search_index(self.index_document, create))
            with self.subTest(f"Search Returns {x} Matches When {x} Documents Indexed"):
                self.verify_search_results(smartseq2_paired_ends_query, x, bundle_ids)

    def test_elasticsearch_exception(self):
        # Test Elasticsearch exception handling by setting an invalid endpoint
        es_logger = logging.getLogger("elasticsearch")
        original_es_level = es_logger.getEffectiveLevel()
        original_es_endpoint = os.getenv("DSS_ES_ENDPOINT", "localhost")

        es_logger.setLevel("ERROR")
        try:
            os.environ['DSS_ES_ENDPOINT'] = "bogus"
            response = self.post_search(smartseq2_paired_ends_query,
                                        requests.codes.internal_server_error)
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

            es_client.index(index=self.dss_index_name,
                            doc_type=ESDocType.doc.name,
                            id=bundle_id,
                            body=index_document)
            bundles.append((bundle_id, bundle_url))
        return bundles

    def post_search(self, query: dict, status_code: int):
        url = str(UrlBuilder()
                  .set(path="/v1/search")
                  .add_query("replica", self.replica_name))
        return self.assertPostResponse(
            path=url,
            json_request_body=dict(es_query=query),
            expected_code=status_code)

    def verify_search_results(self, query, expected_result_length=0, bundles=[], timeout=5):
        timeout_time = timeout + time.time()
        while time.time() <= timeout_time:
            response = self.post_search(query, requests.codes.ok)
            search_response = response.json
            if len(search_response['results']) == expected_result_length:
                break
            elif len(search_response['results']) > expected_result_length:
                self.fail("elasticsearch more results than expected.")
            else:
                time.sleep(0.5)
        else:
            self.fail("elasticsearch failed to return all results.")
        self.assertDictEqual(search_response['es_query'], query)
        self.assertEqual(len(search_response['results']), expected_result_length)
        result_bundles = [(hit['bundle_id'], hit['bundle_url'])
                          for hit in search_response['results']]
        for bundle in bundles:
            self.assertIn(bundle, result_bundles)


class TestGCPSearch(TestSearchBase, unittest.TestCase, ):
    @classmethod
    def setUpClass(cls):
        super().search_setup(dss.Replica.gcp)


class TestAWSSearch(TestSearchBase, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().search_setup(dss.Replica.aws)


if __name__ == "__main__":
    unittest.main()
