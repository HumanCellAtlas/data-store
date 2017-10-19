#!/usr/bin/env python
# coding: utf-8

import json
import logging
import os
import sys
import time
import unittest
import uuid
from urllib.parse import parse_qs, parse_qsl, urlparse, urlsplit
from requests.utils import parse_header_links

import requests

from dss import ESDocType
from dss.util import UrlBuilder

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss.config import IndexSuffix
from dss.events.handlers.index import create_elasticsearch_index
from dss.util.es import ElasticsearchServer, ElasticsearchClient
from tests import get_version
from tests.es import elasticsearch_delete_index
from tests.infra import DSSAssertMixin, ExpectedErrorFields, start_verbose_logging
from tests.infra.server import ThreadedLocalServer
from tests.sample_search_queries import smartseq2_paired_ends_query


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

start_verbose_logging()


class ESInfo:
    server = None


def setUpModule():
    IndexSuffix.name = __name__.rsplit('.', 1)[-1]
    ESInfo.server = ElasticsearchServer()
    os.environ['DSS_ES_PORT'] = str(ESInfo.server.port)


def tearDownModule():
    ESInfo.server.shutdown()
    IndexSuffix.reset()
    os.unsetenv('DSS_ES_PORT')


class TestSearchBase(DSSAssertMixin):
    @classmethod
    def search_setup(cls, replica):
        cls.app = ThreadedLocalServer()
        cls.app.start()
        cls.replica_name = replica.name
        dss.Config.set_config(dss.DeploymentStage.TEST)
        cls.dss_index_name = dss.Config.get_es_index_name(dss.ESIndexType.docs, replica)
        with open(os.path.join(os.path.dirname(__file__), "sample_index_doc.json"), "r") as fh:
            cls.index_document = json.load(fh)

    @classmethod
    def tearDownClass(cls):
        cls.app.shutdown()

    def setUp(self):
        dss.Config.set_config(dss.DeploymentStage.TEST)
        elasticsearch_delete_index(f"*{IndexSuffix.name}")
        create_elasticsearch_index(self.dss_index_name, logger)

    def test_search_post(self):
        bundles = self.populate_search_index(self.index_document, 1)
        self.check_count(smartseq2_paired_ends_query, 1)
        url = self.build_url()
        search_obj = self.assertPostResponse(
            path=url,
            json_request_body=dict(es_query=smartseq2_paired_ends_query),
            expected_code=requests.codes.ok)
        next_url = self.get_next_url(search_obj.response.headers)
        self.assertIsNone(next_url)
        self.verify_search_result(search_obj.json, smartseq2_paired_ends_query, 1, 1)
        self.verify_bundles(search_obj.json['results'], bundles)

    def test_search_returns_no_results_when_no_documents_indexed(self):
        url = self.build_url()
        search_obj = self.assertPostResponse(
            path=url,
            json_request_body=dict(es_query=smartseq2_paired_ends_query),
            expected_code=requests.codes.ok)
        next_url = self.get_next_url(search_obj.response.headers)
        self.assertIsNone(next_url)
        self.verify_search_result(search_obj.json, smartseq2_paired_ends_query, 0)

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
        url = self.build_url()
        search_obj = self.assertPostResponse(
            path=url,
            json_request_body=dict(es_query=query),
            expected_code=requests.codes.ok)
        next_url = self.get_next_url(search_obj.response.headers)
        self.assertIsNone(next_url)
        self.verify_search_result(search_obj.json, query, 0)

    def test_next_url_is_empty_when_all_results_returned(self):
        self.populate_search_index(self.index_document, 1)
        self.check_count(smartseq2_paired_ends_query, 1)
        url = self.build_url()
        search_obj = self.assertPostResponse(
            path=url,
            json_request_body=dict(es_query=smartseq2_paired_ends_query),
            expected_code=requests.codes.ok)
        next_url = self.get_next_url(search_obj.response.headers)
        self.assertIsNone(next_url)

    def test_search_returns_error_when_invalid_query_used(self):
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
                        "match": [
                            "SomethingInvalid", "xxx"
                        ]
                    }
                },
                requests.codes.bad_request
            )
        ]

        self.populate_search_index(self.index_document, 1)
        url = self.build_url()
        for bad_query, error in invalid_query_data:
            with self.subTest(msg="Invalid Queries:", bad_query=bad_query, error=error):
                self.assertPostResponse(
                    path=url,
                    json_request_body=dict(es_query=bad_query),
                    expected_code=error,
                    expected_error=ExpectedErrorFields(code="elasticsearch_bad_request",
                                                       status=error)
                )

    def test_search_returns_N_results_when_N_documents_match_query(self):
        """Create N identical documents. A search query is executed to match the document. All documents created must be
        present in the query results. N is varied across a variety of values.
        """
        #              (total docs, expected len(results) in last search)
        test_matches = [(1, 1),
                        (11, 11),
                        (10000, 0),
                        (10001, 1)]
        bundles = []
        indexed_docs = 0
        url_params = {"per_page": 500}
        for docs, expected_results in test_matches:
            create = docs - indexed_docs
            indexed_docs = docs
            with self.subTest(msg="Find Indexed Documents:", indexed_docs=indexed_docs):
                bundles.extend(self.populate_search_index(self.index_document, create))
                self.check_count(smartseq2_paired_ends_query, indexed_docs)
                search_obj, found_bundles = self.get_search_results(smartseq2_paired_ends_query,
                                                                    url_params=url_params)
                self.verify_search_result(search_obj.json, smartseq2_paired_ends_query, docs, expected_results)
                self.verify_bundles(found_bundles, bundles)

    def test_elasticsearch_bogus_endpoint_exception(self):
        # Test Elasticsearch exception handling by setting an invalid endpoint
        es_logger = logging.getLogger("elasticsearch")
        original_es_level = es_logger.getEffectiveLevel()
        original_es_endpoint = os.getenv("DSS_ES_ENDPOINT", "localhost")

        es_logger.setLevel("ERROR")
        try:
            os.environ['DSS_ES_ENDPOINT'] = "bogus"
            url = self.build_url()
            self.assertPostResponse(
                path=url,
                json_request_body=dict(es_query=smartseq2_paired_ends_query),
                expected_code=requests.codes.not_found,
                expected_error=ExpectedErrorFields(code="not_found",
                                                   status=requests.codes.not_found))
        finally:
            os.environ['DSS_ES_ENDPOINT'] = original_es_endpoint
            es_logger.setLevel(original_es_level)

    def test_next_page_is_delivered_when_next_is_in_query(self):
        bundles = self.populate_search_index(self.index_document, 150)
        self.check_count(smartseq2_paired_ends_query, 150)
        url = self.build_url()
        search_obj = self.assertPostResponse(
            path=url,
            json_request_body=dict(es_query=smartseq2_paired_ends_query),
            expected_code=requests.codes.partial)
        found_bundles = search_obj.json['results']
        next_url = self.get_next_url(search_obj.response.headers)
        self.verify_next_url(next_url)
        self.verify_search_result(search_obj.json, smartseq2_paired_ends_query, len(bundles), 100)
        search_obj = self.assertPostResponse(
            path=self.strip_next_url(next_url),
            json_request_body=dict(es_query=smartseq2_paired_ends_query),
            expected_code=requests.codes.ok)
        found_bundles.extend(search_obj.json['results'])
        self.verify_search_result(search_obj.json, smartseq2_paired_ends_query, len(bundles), 50)
        self.verify_bundles(found_bundles, bundles)

    def test_page_has_N_results_when_per_page_is_N(self):
                        # per_page, expected
        per_page_tests = [(10, 10),
                          (100, 100),
                          (500, 500)]  # max is 500

        self.populate_search_index(self.index_document, 500)
        self.check_count(smartseq2_paired_ends_query, 500)
        for per_page, expected in per_page_tests:
            url = self.build_url({"per_page": per_page})
            with self.subTest(per_page=per_page, expected=expected):
                search_obj = self.assertPostResponse(
                    path=url,
                    json_request_body=dict(es_query=smartseq2_paired_ends_query),
                    expected_code=requests.codes.partial)
                self.verify_search_result(search_obj.json, smartseq2_paired_ends_query, 500, expected)
                next_url = self.get_next_url(search_obj.response.headers)
                self.verify_next_url(next_url, per_page)

    def test_format_is_raw(self):
        bundles = self.populate_search_index(self.index_document, 1)
        self.check_count(smartseq2_paired_ends_query, 1)
        url = self.build_url(url_params={'format': 'raw'})
        search_obj = self.assertPostResponse(
            path=url,
            json_request_body=dict(es_query=smartseq2_paired_ends_query),
            expected_code=requests.codes.ok)
        next_url = self.get_next_url(search_obj.response.headers)
        self.assertIsNone(next_url)
        self.verify_search_result(search_obj.json, smartseq2_paired_ends_query, 1, 1)
        self.verify_bundles(search_obj.json['results'], bundles)
        self.assertEqual(search_obj.json['results'][0]['meta_data'], self.index_document)

    def test_error_returned_when_per_page_is_out_of_range(self):
        expected_error = ExpectedErrorFields(code="illegal_arguments",
                                             status=requests.codes.bad_request,
                                             expect_stacktrace=True)
        per_page_tests = [(9, {'expected_code': requests.codes.bad_request,
                               'expected_error': expected_error}),  # min is 10
                          (501, {'expected_code': requests.codes.bad_request,
                                 'expected_error': expected_error})]  # max is 500
        for per_page, expected in per_page_tests:
            url = self.build_url({"per_page": per_page})
            with self.subTest(per_page=per_page, expected=expected):
                self.assertPostResponse(
                    path=url,
                    json_request_body=dict(es_query=smartseq2_paired_ends_query),
                    **expected)

    def test_search_session_expired_when_session_deleted(self):
        self.populate_search_index(self.index_document, 20)
        self.check_count(smartseq2_paired_ends_query, 20)
        url = self.build_url({"per_page": 10})
        search_obj = self.assertPostResponse(
            path=url,
            json_request_body=dict(es_query=smartseq2_paired_ends_query),
            expected_code=requests.codes.partial)
        self.verify_search_result(search_obj.json, smartseq2_paired_ends_query, 20, 10)
        next_url = self.get_next_url(search_obj.response.headers)
        scroll_id = self.verify_next_url(next_url, 10)
        es_client = ElasticsearchClient.get(logger)
        es_client.clear_scroll(scroll_id)
        self.assertPostResponse(
            path=self.strip_next_url(next_url),
            json_request_body=dict(es_query=smartseq2_paired_ends_query),
            expected_code=requests.codes.not_found,
            expected_error=ExpectedErrorFields(code="elasticsearch_context_not_found",
                                               status=requests.codes.not_found))

    def populate_search_index(self, index_document: dict, count: int) -> list:
        es_client = ElasticsearchClient.get(logger)
        bundles = []
        for i in range(count):
            bundle_uuid = str(uuid.uuid4())
            version = get_version()
            index_document['manifest']['version'] = version
            bundle_id = f"{bundle_uuid}.{version}"
            bundle_url = (f"https://localhost:{self.app._port}"
                          f"/v1/bundles/{bundle_uuid}?version={version}&replica={self.replica_name}")
            es_client.index(index=self.dss_index_name,
                            doc_type=ESDocType.doc.name,
                            id=bundle_id,
                            body=index_document)
            bundles.append((bundle_id, bundle_url))
        return bundles

    def build_url(self, url_params=None):
        url = UrlBuilder().set(path="/v1/search").add_query("replica", self.replica_name)
        if url_params:
            for param in url_params:
                url = url.add_query(param, url_params[param])
        return str(url)

    def verify_search_result(self, search_json, es_query, total_hits, expected_result_length=0):
        self.assertDictEqual(search_json['es_query'], es_query)
        self.assertEqual(len(search_json['results']), expected_result_length)
        self.assertEqual(search_json['total_hits'], total_hits)

    def verify_bundles(self, found_bundles, expected_bundles):
        result_bundles = [(hit['bundle_id'], hit['bundle_url'])
                          for hit in found_bundles]
        for bundle in expected_bundles:
            self.assertIn(bundle, result_bundles)

    def verify_next_url(self, next_url, per_page=100):
        parsed_url = urlparse(next_url)
        self.assertEqual(parsed_url.path, "/v1/search")
        parsed_q = parse_qs(parsed_url.query)
        self.assertEqual(parsed_q['replica'], [self.replica_name])
        self.assertIn('_scroll_id', parsed_q.keys())
        self.assertEqual(parsed_q['per_page'], [str(per_page)])
        return parsed_q['_scroll_id'][0]

    @staticmethod
    def strip_next_url(next_url: str) -> str:
        """
        The API returns a fully-qualified url, but hitting self.assert* requires just the path.  This method just strips
        the scheme and the host from the url.
        """
        parsed = urlsplit(next_url)
        return str(UrlBuilder().set(path=parsed.path, query=parse_qsl(parsed.query), fragment=parsed.fragment))

    @staticmethod
    def get_next_url(headers):
        links = headers.get("Link")
        if links is not None:
            for link in parse_header_links(links):
                if link['rel'] == 'next':
                    return link["url"]
        else:
            return links

    def get_search_results(self, es_query, url_params=None):
        if not url_params:
            url_params = {}
        url = self.build_url(url_params)
        found_bundles = []
        while True:
            search_obj = self.assertPostResponse(
                path=self.strip_next_url(url),
                json_request_body=dict(es_query=es_query),
                expected_code=(requests.codes.ok, requests.codes.partial))
            found_bundles.extend(search_obj.json['results'])
            if search_obj.response.status_code == 200:
                break
            url = self.get_next_url(search_obj.response.headers)
        return search_obj, found_bundles

    def check_count(self, es_query, expected_count, timeout=5):
        es_client = ElasticsearchClient.get(logger)
        timeout_time = timeout + time.time()
        while time.time() <= timeout_time:
            count_resp = es_client.count(index=self.dss_index_name,
                                         doc_type=ESDocType.doc.name,
                                         body=es_query)
            if count_resp['count'] == expected_count:
                break
            else:
                time.sleep(0.5)
        else:
            self.fail("elasticsearch failed to return all results.")


class TestGCPSearch(TestSearchBase, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().search_setup(dss.Replica.gcp)


class TestAWSSearch(TestSearchBase, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().search_setup(dss.Replica.aws)


if __name__ == "__main__":
    unittest.main()
