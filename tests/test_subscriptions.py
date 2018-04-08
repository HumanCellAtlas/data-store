#!/usr/bin/env python
# coding: utf-8

import json
import logging
import os
import sys
import unittest
import uuid
from io import open
from contextlib import contextmanager

import connexion.apis.abstract
import requests

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss.index.es import ElasticsearchClient
from dss.index.es.document import BundleDocument
from dss.index.es.manager import IndexManager
from dss.notify.notification import Endpoint
from dss.logging import configure_test_logging
from dss.util import UrlBuilder
from tests import get_auth_header, get_bundle_fqid
from tests.infra import DSSAssertMixin, testmode
from tests.infra.elasticsearch_test_case import ElasticsearchTestCase
from tests.infra.server import ThreadedLocalServer
from tests.sample_search_queries import smartseq2_paired_ends_v2_or_v3_query


logger = logging.getLogger(__name__)


def setUpModule():
    configure_test_logging()


@testmode.standalone
class TestSubscriptionsBase(ElasticsearchTestCase, DSSAssertMixin):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.app = ThreadedLocalServer()
        cls.app.start()
        dss.Config.set_config(dss.BucketConfig.TEST)

        with open(os.path.join(os.path.dirname(__file__), "sample_v3_index_doc.json"), "r") as fh:
            cls.index_document = BundleDocument(cls.replica, get_bundle_fqid())
            cls.index_document.update(json.load(fh))

    @classmethod
    def tearDownClass(cls):
        cls.app.shutdown()
        super().tearDownClass()

    def setUp(self):
        super().setUp()
        self.alias_name = dss.Config.get_es_alias_name(dss.ESIndexType.docs, self.replica)
        self.sub_index_name = dss.Config.get_es_index_name(dss.ESIndexType.subscriptions, self.replica)
        shape_identifier = self.index_document.get_shape_descriptor()
        self.doc_index_name = dss.Config.get_es_index_name(dss.ESIndexType.docs, self.replica, shape_identifier)
        es_client = ElasticsearchClient.get()
        IndexManager.create_index(es_client, self.replica, self.doc_index_name)
        es_client.index(index=self.doc_index_name,
                        doc_type=dss.ESDocType.doc.name,
                        id=str(uuid.uuid4()),
                        body=self.index_document,
                        refresh=True)
        self.endpoint = Endpoint(callback_url="https://example.com",
                                 method="POST",
                                 encoding="application/json",
                                 form_fields={'foo': 'bar'},
                                 payload_form_field='baz')
        self.sample_percolate_query = smartseq2_paired_ends_v2_or_v3_query

    def test_auth_errors(self):
        url = str(UrlBuilder()
                  .set(path="/v1/subscriptions/" + str(uuid.uuid4()))
                  .add_query("replica", self.replica.name))

        # Unauthorized email
        with self.throw_403():
            resp_obj = self.assertGetResponse(url, requests.codes.forbidden, headers=get_auth_header())
        self.assertEqual(resp_obj.response.headers['Content-Type'], "application/problem+json")

        # Gibberish auth header
        resp_obj = self.assertGetResponse(url, requests.codes.unauthorized, headers=get_auth_header(False))
        self.assertEqual(resp_obj.response.headers['Content-Type'], "application/problem+json")

        # No auth header
        self.assertGetResponse(url, requests.codes.unauthorized)

    def test_put(self):
        uuid_ = self._put_subscription()

        es_client = ElasticsearchClient.get()
        response = es_client.get(index=self.doc_index_name,
                                 doc_type=dss.ESDocType.query.name,
                                 id=uuid_)
        registered_query = response['_source']
        self.assertEqual(self.sample_percolate_query, registered_query)

    def test_validation(self):
        with self.subTest("Missing URL"):
            self._put_subscription(expect_code=400,
                                   endpoint={})
        with self.subTest("Invalid form field value"):
            self._put_subscription(expect_code=400,
                                   endpoint=Endpoint(callback_url=self.endpoint.callback_url,
                                                     form_fields={'foo': 1}))
        with self.subTest("Invalid encoding"):
            self._put_subscription(expect_code=400,
                                   endpoint=Endpoint(callback_url=self.endpoint.callback_url,
                                                     encoding='foo'))
        with self.subTest("Invalid method"):
            self._put_subscription(expect_code=400,
                                   endpoint=Endpoint(callback_url=self.endpoint.callback_url,
                                                     method='foo'))

    def test_subscription_registration_succeeds_when_query_does_not_match_mappings(self):
        # It is now possible to register a subscription query before the mapping
        # of the field exists in the mappings (and may never exist in the mapppings)
        es_query = {
            "query": {
                "bool": {
                    "must": [{
                        "match": {
                            "assay.fake_field": "this is a negative test"
                        }
                    }],
                }
            }
        }

        url = str(UrlBuilder()
                  .set(path="/v1/subscriptions")
                  .add_query("replica", self.replica.name))
        resp_obj = self.assertPutResponse(
            url,
            requests.codes.created,
            json_request_body=dict(
                es_query=es_query,
                **self.endpoint.to_dict()),
            headers=get_auth_header()
        )
        self.assertIn('uuid', resp_obj.json)

    def test_get(self):
        find_uuid = self._put_subscription()

        # Normal request
        url = str(UrlBuilder()
                  .set(path="/v1/subscriptions/" + str(find_uuid))
                  .add_query("replica", self.replica.name))
        resp_obj = self.assertGetResponse(
            url,
            requests.codes.okay,
            headers=get_auth_header())
        json_response = resp_obj.json
        self.assertEqual(self.sample_percolate_query, json_response['es_query'])
        self.assertEqual(self.endpoint, Endpoint.from_subscription(json_response))

        # Forbidden request w/ previous url
        with self.throw_403():
            self.assertGetResponse(
                url,
                requests.codes.forbidden,
                headers=get_auth_header())

        # File not found request
        url = str(UrlBuilder()
                  .set(path="/v1/subscriptions/" + str(uuid.uuid4()))
                  .add_query("replica", self.replica.name))
        self.assertGetResponse(
            url,
            requests.codes.not_found,
            headers=get_auth_header())

    def test_find(self):
        num_additions = 25
        for _ in range(num_additions):
            self._put_subscription()
        url = str(UrlBuilder()
                  .set(path="/v1/subscriptions")
                  .add_query("replica", self.replica.name))
        resp_obj = self.assertGetResponse(
            url,
            requests.codes.okay,
            headers=get_auth_header())
        json_response = resp_obj.json
        self.assertEqual(self.sample_percolate_query, json_response['subscriptions'][0]['es_query'])
        self.assertEqual(self.endpoint, Endpoint.from_subscription(json_response['subscriptions'][0]))
        self.assertEqual(num_additions, len(json_response['subscriptions']))

    def test_delete(self):
        find_uuid = self._put_subscription()
        url = str(UrlBuilder()
                  .set(path="/v1/subscriptions/" + find_uuid)
                  .add_query("replica", self.replica.name))

        # Forbidden delete request
        with self.throw_403():
            self.assertDeleteResponse(url, requests.codes.forbidden, headers=get_auth_header())

        # Authorized delete
        self.assertDeleteResponse(url, requests.codes.okay, headers=get_auth_header())

        # 1. Check that previous delete worked
        # 2. Check that we can't delete files that don't exist
        self.assertDeleteResponse(url, requests.codes.not_found, headers=get_auth_header())

    def _put_subscription(self, endpoint=None, expect_code=None):
        url = str(UrlBuilder()
                  .set(path="/v1/subscriptions")
                  .add_query("replica", self.replica.name))
        if endpoint is None:
            endpoint = self.endpoint
        if isinstance(endpoint, Endpoint):
            endpoint = endpoint.to_dict()
        resp_obj = self.assertPutResponse(
            url,
            expect_code or requests.codes.created,
            json_request_body=dict(endpoint, es_query=self.sample_percolate_query),
            headers=get_auth_header()
        )
        return resp_obj.json.get('uuid')

    @contextmanager
    def throw_403(self):
        orig_testing_403 = connexion.apis.abstract.Operation.testing_403
        try:
            connexion.apis.abstract.Operation.testing_403 = True
            yield
        finally:
            connexion.apis.abstract.Operation.testing_403 = orig_testing_403


class TestGCPSubscription(TestSubscriptionsBase):
    replica = dss.Replica.gcp


class TestAWSSubscription(TestSubscriptionsBase):
    replica = dss.Replica.aws

# Prevent unittest's discovery from attempting to discover the base test class. The alterative, not inheriting
# TestCase in the base class, is too inconvenient because it interferes with auto-complete and generates PEP-8
# warnings about the camel case methods.
#
del TestSubscriptionsBase


if __name__ == '__main__':
    unittest.main()
