#!/usr/bin/env python
# coding: utf-8

import json
import logging
import os
import sys
import unittest
import uuid
from io import open
import hashlib
import requests

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss.config import Replica
from dss.index.es import ElasticsearchClient
from dss.index.es.document import BundleDocument
from dss.index.es.manager import IndexManager
from dss.notify.notification import Endpoint
from dss.logging import configure_test_logging
from dss.util import UrlBuilder
from dss.subscriptions_v2 import count_subscriptions_for_owner, put_subscription, delete_subscription, SubscriptionData
from tests import get_auth_header, get_bundle_fqid
from tests.infra import DSSAssertMixin, testmode, TestAuthMixin
from tests.infra.elasticsearch_test_case import ElasticsearchTestCase
from tests.infra.server import ThreadedLocalServer
from tests.sample_search_queries import smartseq2_paired_ends_vx_query


logger = logging.getLogger(__name__)


def setUpModule():
    configure_test_logging()


@testmode.integration
class TestSubscriptionsBase(ElasticsearchTestCase, TestAuthMixin, DSSAssertMixin):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.app = ThreadedLocalServer()
        cls.app.start()
        dss.Config.set_config(dss.BucketConfig.TEST)

        with open(os.path.join(os.path.dirname(__file__), "sample_vx_index_doc.json"), "r") as fh:
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
        shape_identifier = self.index_document._get_shape_descriptor()
        shape_identifier = hashlib.sha1(f"{shape_identifier}".encode("utf-8")).hexdigest()
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
        self.sample_percolate_query = smartseq2_paired_ends_vx_query
        self.hmac_key_id = 'dss_test'
        self.hmac_secret_key = '23/33'

    def test_auth_errors(self):
        url = str(UrlBuilder()
                  .set(path="/v1/subscriptions/" + str(uuid.uuid4()))
                  .add_query("replica", self.replica.name)
                  .add_query("subscription_type", "elasticsearch"))
        self._test_auth_errors('get', url)

    def test_put(self):
        try:
            uuid_ = self._put_subscription()

            es_client = ElasticsearchClient.get()
            response = es_client.get(index=self.doc_index_name,
                                     doc_type=dss.ESDocType.query.name,
                                     id=uuid_)
            registered_query = response['_source']
            self.assertEqual(self.sample_percolate_query, registered_query)
        finally:
            self._cleanup_subscription(uuid_)

    def test_db_count_subscriptions_for_owner(self):
        """Test dynamoDB helper functions used to store and retrieve subscription information."""
        owner = 'email@email.com'
        subscription_uuid = str(uuid.uuid4())

        for r in Replica:
            replica = r.name
            subscription_doc = {SubscriptionData.OWNER: owner,
                                SubscriptionData.UUID: subscription_uuid,
                                SubscriptionData.REPLICA: replica}

            with self.subTest(f'DynamoDB: Counting initial (zero) subscriptions for {owner} in {replica}.'):
                assert isinstance(count_subscriptions_for_owner(Replica[replica], owner), int)
                assert count_subscriptions_for_owner(Replica[replica], owner) == 0

            with self.subTest(f'DynamoDB: put_subscription for {owner} in {replica} and check count is one.'):
                put_subscription(subscription_doc)
                assert isinstance(count_subscriptions_for_owner(Replica[replica], owner), int)
                assert count_subscriptions_for_owner(Replica[replica], owner) == 1

            with self.subTest(f'DynamoDB: Counting (zero) subscriptions for {owner} in {replica} after deletion.'):
                delete_subscription(Replica[replica], owner=owner, uuid=subscription_uuid)
                assert isinstance(count_subscriptions_for_owner(Replica[replica], owner), int)
                assert count_subscriptions_for_owner(Replica[replica], owner) == 0

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
        with self.subTest("Invalid attachment type"):
            self._put_subscription(expect_code=400,
                                   attachments={'foo': dict(type='foo', expression='bar')})
        with self.subTest("Invalid attachment expression"):
            self._put_subscription(expect_code=400,
                                   attachments={'foo': dict(type='jmespath', expression='')})

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
                  .add_query("replica", self.replica.name)
                  .add_query("subscription_type", "elasticsearch"))
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
        try:
            find_uuid = self._put_subscription()

            # Normal request
            url = str(UrlBuilder()
                      .set(path="/v1/subscriptions/" + str(find_uuid))
                      .add_query("replica", self.replica.name)
                      .add_query("subscription_type", "elasticsearch"))
            resp_obj = self.assertGetResponse(
                url,
                requests.codes.okay,
                headers=get_auth_header())
            json_response = resp_obj.json
            self.assertEqual(self.sample_percolate_query, json_response['es_query'])
            self.assertEqual(self.endpoint, Endpoint.from_subscription(json_response))
            self.assertEquals(self.hmac_key_id, json_response['hmac_key_id'])
            self.assertNotIn('hmac_secret_key', json_response)
        finally:
            self._cleanup_subscription(find_uuid)

        # File not found request
        url = str(UrlBuilder()
                  .set(path="/v1/subscriptions/" + str(uuid.uuid4()))
                  .add_query("replica", self.replica.name)
                  .add_query("subscription_type", "elasticsearch"))
        self.assertGetResponse(
            url,
            requests.codes.not_found,
            headers=get_auth_header())

    def test_find(self):
        try:
            num_additions = 3
            uuids = list()
            for _ in range(num_additions):
                uuids.append(self._put_subscription())
            url = str(UrlBuilder()
                      .set(path="/v1/subscriptions")
                      .add_query("replica", self.replica.name)
                      .add_query("subscription_type", "elasticsearch"))
            resp_obj = self.assertGetResponse(
                url,
                requests.codes.okay,
                headers=get_auth_header())
            json_response = resp_obj.json
            self.assertEqual(self.sample_percolate_query, json_response['subscriptions'][0]['es_query'])
            self.assertEqual(self.hmac_key_id, json_response['subscriptions'][0]['hmac_key_id'])
            self.assertEqual(self.endpoint, Endpoint.from_subscription(json_response['subscriptions'][0]))
            self.assertNotIn('hmac_secret_key', json_response['subscriptions'][0])
            self.assertEqual(num_additions, len(json_response['subscriptions']))
        finally:
            for _uuid in uuids:
                self._cleanup_subscription(_uuid)

    def test_delete(self):
        find_uuid = self._put_subscription()
        url = str(UrlBuilder()
                  .set(path="/v1/subscriptions/" + find_uuid)
                  .add_query("replica", self.replica.name)
                  .add_query("subscription_type", "elasticsearch"))

        # Authorized delete
        self.assertDeleteResponse(url, requests.codes.okay, headers=get_auth_header())

        # 1. Check that previous delete worked
        # 2. Check that we can't delete files that don't exist
        self.assertDeleteResponse(url, requests.codes.not_found, headers=get_auth_header())

    def _put_subscription(self, endpoint=None, expect_code=None, attachments=None):
        url = str(UrlBuilder()
                  .set(path="/v1/subscriptions")
                  .add_query("replica", self.replica.name)
                  .add_query("subscription_type", "elasticsearch"))
        if endpoint is None:
            endpoint = self.endpoint
        if isinstance(endpoint, Endpoint):
            endpoint = endpoint.to_dict()
        json_request_body = dict(endpoint, es_query=self.sample_percolate_query, hmac_key_id=self.hmac_key_id,
                                 hmac_secret_key=self.hmac_secret_key)
        if attachments is not None:
            json_request_body['attachments'] = attachments
        resp_obj = self.assertPutResponse(
            url,
            expect_code or requests.codes.created,
            json_request_body=json_request_body,
            headers=get_auth_header()
        )
        return resp_obj.json.get('uuid')

    def _cleanup_subscription(self, uuid, subscription_type=None):
        if not subscription_type:
            subscription_type = 'elasticsearch'
        url = (UrlBuilder()
               .set(path=f"/v1/subscriptions/{uuid}")
               .add_query("replica", self.replica.name)
               .add_query('subscription_type', subscription_type))
        self.assertDeleteResponse(url, requests.codes.okay, headers=get_auth_header())


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
