#!/usr/bin/env python
# coding: utf-8

import json
import logging
import os
import sys
import unittest
import uuid
from contextlib import contextmanager
from io import open

import connexion.apis.abstract
import google.auth
import google.auth.transport.requests
import requests

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..')) # noqa
sys.path.insert(0, pkg_root) # noqa

import dss
from dss.config import IndexSuffix
from dss.util import UrlBuilder
from dss.util.es import ElasticsearchClient, ElasticsearchServer, get_elasticsearch_index
from tests.es import elasticsearch_delete_index
from tests.infra import DSSAssertMixin, ExpectedErrorFields
from tests.infra.server import ThreadedLocalServer

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

class TestSubscriptionsBase(DSSAssertMixin):
    @classmethod
    def subsciption_setup(cls, replica):
        cls.replica = replica
        cls.app = ThreadedLocalServer()
        cls.app.start()

    @classmethod
    def tearDownClass(cls):
        cls.app.shutdown()

    def setUp(self):
        os.environ['DSS_ES_ENDPOINT'] = os.getenv('DSS_ES_ENDPOINT', "127.0.0.1")

        dss.Config.set_config(dss.BucketConfig.TEST)

        logger.debug("Setting up Elasticsearch")
        es_client = ElasticsearchClient.get(logger)
        elasticsearch_delete_index(f"*{IndexSuffix.name}")
        index_mapping = {  # Need to make a mapping for the percolator type before we add any.
            "mappings": {
                dss.ESDocType.query.name: {
                    "properties": {
                        "query": {
                            "type": "percolator"
                        }
                    }
                }
            }
        }
        index_name = dss.Config.get_es_index_name(dss.ESIndexType.docs, self.replica)
        get_elasticsearch_index(es_client, index_name, logger, index_mapping)

        with open(os.path.join(os.path.dirname(__file__), "sample_index_doc.json"), "r") as fh:
            index_document = json.load(fh)

        self.callback_url = "https://example.com"
        with open(os.path.join(os.path.dirname(__file__), "sample_query.json"), "r") as fh:
            self.sample_percolate_query = json.load(fh)

        es_client.index(index=index_name,
                        doc_type=dss.ESDocType.doc.name,
                        id=str(uuid.uuid4()),
                        body=index_document,
                        refresh=True)

    def test_auth_errors(self):
        url = str(UrlBuilder()
                  .set(path="/v1/subscriptions/" + str(uuid.uuid4()))
                  .add_query("replica", self.replica.name))

        # Unauthorized email
        with self.throw_403():
            resp_obj = self.assertGetResponse(url, requests.codes.forbidden, headers=self._get_auth_header())
        self.assertEqual(resp_obj.response.headers['Content-Type'], "application/problem+json")

        # Gibberish auth header
        resp_obj = self.assertGetResponse(url, requests.codes.unauthorized, headers=self._get_auth_header(False))
        self.assertEqual(resp_obj.response.headers['Content-Type'], "application/problem+json")

        # No auth header
        resp_obj = self.assertGetResponse(url, requests.codes.unauthorized)

    def test_put(self):
        uuid_ = self._put_subscription()

        es_client = ElasticsearchClient.get(logger)
        response = es_client.get(index=dss.Config.get_es_index_name(dss.ESIndexType.docs, self.replica),
                                 doc_type=dss.ESDocType.query.name,
                                 id=uuid_)
        registered_query = response['_source']
        self.assertEqual(self.sample_percolate_query, registered_query)

    def test_subscription_registration_fails_when_query_does_not_match_schema(self):
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
        self._put_subscription()

        url = str(UrlBuilder()
                  .set(path="/v1/subscriptions")
                  .add_query("replica", self.replica.name))
        resp_obj = self.assertPutResponse(
            url,
            requests.codes.internal_server_error,
            json_request_body=dict(
                es_query=es_query,
                callback_url=self.callback_url),
            headers=self._get_auth_header(),
            expected_error=ExpectedErrorFields(
                code="elasticsearch_error",
                status=requests.codes.internal_server_error,
                expect_stacktrace=True)
        )
        self.assertIn("No field mapping can be found for the field with name [assay.fake_field]",
                      resp_obj.json['stacktrace'])
        self.assertEqual(resp_obj.json['title'], 'Unable to register elasticsearch percolate query!')

    def test_get(self):
        find_uuid = self._put_subscription()

        # Normal request
        url = str(UrlBuilder()
                  .set(path="/v1/subscriptions/" + str(find_uuid))
                  .add_query("replica", self.replica.name))
        resp_obj = self.assertGetResponse(
            url,
            requests.codes.okay,
            headers=self._get_auth_header())
        json_response = resp_obj.json
        self.assertEqual(self.sample_percolate_query, json_response['es_query'])
        self.assertEqual(self.callback_url, json_response['callback_url'])

        # Forbidden request w/ previous url
        with self.throw_403():
            self.assertGetResponse(
                url,
                requests.codes.forbidden,
                headers=self._get_auth_header())

        # File not found request
        url = str(UrlBuilder()
                  .set(path="/v1/subscriptions/" + str(uuid.uuid4()))
                  .add_query("replica", self.replica.name))
        self.assertGetResponse(
            url,
            requests.codes.not_found,
            headers=self._get_auth_header())

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
            headers=self._get_auth_header())
        json_response = resp_obj.json
        self.assertEqual(self.sample_percolate_query, json_response['subscriptions'][0]['es_query'])
        self.assertEqual(self.callback_url, json_response['subscriptions'][0]['callback_url'])
        self.assertEqual(NUM_ADDITIONS, len(json_response['subscriptions']))

    def test_delete(self):
        find_uuid = self._put_subscription()
        url = str(UrlBuilder()
                  .set(path="/v1/subscriptions/" + find_uuid)
                  .add_query("replica", self.replica.name))

        # Forbidden delete request
        with self.throw_403():
            self.assertDeleteResponse(url, requests.codes.forbidden, headers=self._get_auth_header())

        # Authorized delete
        self.assertDeleteResponse(url, requests.codes.okay, headers=self._get_auth_header())

        # 1. Check that previous delete worked
        # 2. Check that we can't delete files that don't exist
        self.assertDeleteResponse(url, requests.codes.not_found, headers=self._get_auth_header())

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
            headers=self._get_auth_header()
        )
        uuid_ = resp_obj.json['uuid']
        return uuid_

    def _get_auth_header(self, real_header=True):
        credentials, project_id = google.auth.default(scopes=["https://www.googleapis.com/auth/userinfo.email"])

        r = google.auth.transport.requests.Request()
        credentials.refresh(r)
        r.session.close()

        token = credentials.token if real_header else str(uuid.uuid4())

        return {"Authorization": f"Bearer {token}"}

    @contextmanager
    def throw_403(self):
        orig_testing_403 = connexion.apis.abstract.Operation.testing_403
        try:
            connexion.apis.abstract.Operation.testing_403 = True
            yield
        finally:
            connexion.apis.abstract.Operation.testing_403 = orig_testing_403


class TestGCPSubscription(TestSubscriptionsBase, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().subsciption_setup(dss.Replica.gcp)


class TestAWSSubscription(TestSubscriptionsBase, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().subsciption_setup(dss.Replica.aws)


if __name__ == '__main__':
    unittest.main()
