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

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..')) # noqa
sys.path.insert(0, pkg_root) # noqa

import dss
from dss.config import IndexSuffix
from dss.events.handlers.index import BundleDocument
from dss.storage.index import IndexManager
from dss.util import UrlBuilder
from dss.util.es import ElasticsearchClient, ElasticsearchServer
from tests import get_auth_header, get_bundle_fqid
from tests.es import elasticsearch_delete_index, clear_indexes
from tests.infra import DSSAssertMixin, testmode
from tests.infra.server import ThreadedLocalServer
from tests.sample_search_queries import smartseq2_paired_ends_v2_or_v3_query

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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


class TestSubscriptionsBase(unittest.TestCase, DSSAssertMixin):
    @classmethod
    def setUpClass(cls):
        cls.app = ThreadedLocalServer()
        cls.app.start()
        dss.Config.set_config(dss.BucketConfig.TEST)
        os.environ['DSS_ES_ENDPOINT'] = os.getenv('DSS_ES_ENDPOINT', "127.0.0.1")

        with open(os.path.join(os.path.dirname(__file__), "sample_v3_index_doc.json"), "r") as fh:
            index_document = BundleDocument(
                cls.replica,
                get_bundle_fqid(),
                logger,
            )
            index_document.update(json.load(fh))

        logger.debug("Setting up Elasticsearch")
        es_client = ElasticsearchClient.get(logger)
        index_shape_identifier = index_document.get_shape_descriptor()
        cls.alias_name = dss.Config.get_es_alias_name(dss.ESIndexType.docs, cls.replica)
        cls.sub_index_name = dss.Config.get_es_index_name(dss.ESIndexType.subscriptions, cls.replica)
        cls.doc_index_name = dss.Config.get_es_index_name(dss.ESIndexType.docs, cls.replica, index_shape_identifier)
        IndexManager.create_index(es_client, cls.replica, cls.doc_index_name)
        es_client.index(index=cls.doc_index_name,
                        doc_type=dss.ESDocType.doc.name,
                        id=str(uuid.uuid4()),
                        body=index_document,
                        refresh=True)

    @classmethod
    def tearDownClass(cls):
        elasticsearch_delete_index(f"*{IndexSuffix.name}")
        cls.app.shutdown()

    def setUp(self):
        self.callback_url = "https://example.com"
        self.sample_percolate_query = smartseq2_paired_ends_v2_or_v3_query

    def tearDown(self):
        clear_indexes([self.alias_name, self.sub_index_name],
                      [dss.ESDocType.doc.name, dss.ESDocType.query.name, dss.ESDocType.subscription.name])

    @testmode.standalone
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
        resp_obj = self.assertGetResponse(url, requests.codes.unauthorized)

    @testmode.standalone
    def test_put(self):
        uuid_ = self._put_subscription()

        es_client = ElasticsearchClient.get(logger)
        response = es_client.get(index=self.doc_index_name,
                                 doc_type=dss.ESDocType.query.name,
                                 id=uuid_)
        registered_query = response['_source']
        self.assertEqual(self.sample_percolate_query, registered_query)

    @testmode.standalone
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
                callback_url=self.callback_url),
            headers=get_auth_header()
        )
        self.assertIn('uuid', resp_obj.json)

    @testmode.standalone
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
        self.assertEqual(self.callback_url, json_response['callback_url'])

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

    @testmode.standalone
    def test_find(self):
        NUM_ADDITIONS = 25
        for _ in range(NUM_ADDITIONS):
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
        self.assertEqual(self.callback_url, json_response['subscriptions'][0]['callback_url'])
        self.assertEqual(NUM_ADDITIONS, len(json_response['subscriptions']))

    @testmode.standalone
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

    def _put_subscription(self):
        url = str(UrlBuilder()
                  .set(path="/v1/subscriptions")
                  .add_query("replica", self.replica.name))
        resp_obj = self.assertPutResponse(
            url,
            requests.codes.created,
            json_request_body=dict(
                es_query=self.sample_percolate_query,
                callback_url=self.callback_url),
            headers=get_auth_header()
        )
        uuid_ = resp_obj.json['uuid']
        return uuid_

    @contextmanager
    def throw_403(self):
        orig_testing_403 = connexion.apis.abstract.Operation.testing_403
        try:
            connexion.apis.abstract.Operation.testing_403 = True
            yield
        finally:
            connexion.apis.abstract.Operation.testing_403 = orig_testing_403


class TestGCPSubscription(TestSubscriptionsBase, unittest.TestCase):
    replica = dss.Replica.gcp


class TestAWSSubscription(TestSubscriptionsBase, unittest.TestCase):
    replica = dss.Replica.aws


if __name__ == '__main__':
    unittest.main()
