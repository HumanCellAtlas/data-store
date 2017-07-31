#!/usr/bin/env python
# coding: utf-8

import json
import logging
import os
import sys
import unittest
import uuid
from io import open

import google.auth
import google.auth.transport.requests
import requests

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..')) # noqa
sys.path.insert(0, pkg_root) # noqa

import dss
from dss import DSS_ELASTICSEARCH_INDEX_NAME, DSS_ELASTICSEARCH_DOC_TYPE, DSS_ELASTICSEARCH_QUERY_TYPE
from dss.config import DeploymentStage, override_email_config
from dss.util import UrlBuilder
from dss.util.es import ElasticsearchClient, get_elasticsearch_index
from tests.es import check_start_elasticsearch_service, close_elasticsearch_connections, elasticsearch_delete_index
from tests.infra import DSSAsserts

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class TestSubscriptions(unittest.TestCase, DSSAsserts):

    def setUp(self):
        os.environ['DSS_ES_ENDPOINT'] = os.getenv('DSS_ES_ENDPOINT', "localhost")

        self.app = dss.create_app().app.test_client()
        dss.Config.set_config(dss.DeploymentStage.TEST)

        logger.debug("Setting up Elasticsearch")
        es_client = ElasticsearchClient.get(logger)
        es_client.indices.delete(index="_all", ignore=[404])  # Disregard if no indices - don't error.
        index_mapping = {  # Need to make a mapping for the percolator type before we add any.
            "mappings": {
                DSS_ELASTICSEARCH_QUERY_TYPE: {
                    "properties": {
                        "query": {
                            "type": "percolator"
                        }
                    }
                }
            }
        }
        get_elasticsearch_index(es_client, DSS_ELASTICSEARCH_INDEX_NAME, logger, index_mapping)

        with open(os.path.join(os.path.dirname(__file__), "sample_index_doc.json"), "r") as fh:
            index_document = json.load(fh)

        self.callback_url = "https://example.com"
        with open(os.path.join(os.path.dirname(__file__), "sample_query.json"), "r") as fh:
            self.sample_percolate_query = json.load(fh)

        es_client.index(index=DSS_ELASTICSEARCH_INDEX_NAME,
                        doc_type=DSS_ELASTICSEARCH_DOC_TYPE,
                        id=str(uuid.uuid4()),
                        body=index_document,
                        refresh=True)

    def test_auth_errors(self):
        url = str(UrlBuilder()
                  .set(path="/v1/subscriptions/" + str(uuid.uuid4()))
                  .add_query("replica", "aws"))

        with override_email_config(DeploymentStage.NO_AUTH):
            resp_obj = self.assertGetResponse(url, requests.codes.forbidden, headers=self._get_auth_header())
        self.assertEqual(resp_obj.response.headers['Content-Type'], "application/problem+json")

        resp_obj = self.assertGetResponse(url, requests.codes.unauthorized, headers=self._get_auth_header(False))
        self.assertEqual(resp_obj.response.headers['Content-Type'], "application/problem+json")

    def test_put(self):
        uuid_ = self._put_subscription()

        es_client = ElasticsearchClient.get(logger)
        response = es_client.get(index=DSS_ELASTICSEARCH_INDEX_NAME,
                                 doc_type=DSS_ELASTICSEARCH_QUERY_TYPE,
                                 id=uuid_)
        registered_query = response['_source']
        self.assertEqual(self.sample_percolate_query, registered_query)

    def test_get(self):
        find_uuid = self._put_subscription()

        # Normal request
        url = str(UrlBuilder()
                  .set(path="/v1/subscriptions/" + str(find_uuid))
                  .add_query("replica", "aws"))
        resp_obj = self.assertGetResponse(
            url,
            requests.codes.okay,
            headers=self._get_auth_header())
        json_response = resp_obj.json
        self.assertEqual(self.sample_percolate_query, json_response['query'])
        self.assertEqual(self.callback_url, json_response['callback_url'])

        # Forbidden request w/ previous url
        with override_email_config(DeploymentStage.NO_AUTH):
            self.assertGetResponse(
                url,
                requests.codes.forbidden,
                headers=self._get_auth_header())

        # File not found request
        url = str(UrlBuilder()
                  .set(path="/v1/subscriptions/" + str(uuid.uuid4()))
                  .add_query("replica", "aws"))
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
                  .add_query("replica", "aws"))
        resp_obj = self.assertGetResponse(
            url,
            requests.codes.okay,
            headers=self._get_auth_header())
        json_response = resp_obj.json
        self.assertEqual(self.sample_percolate_query, json_response['subscriptions'][0]['query'])
        self.assertEqual(self.callback_url, json_response['subscriptions'][0]['callback_url'])
        self.assertEqual(NUM_ADDITIONS, len(json_response['subscriptions']))

    def test_delete(self):
        find_uuid = self._put_subscription()
        url = str(UrlBuilder()
                  .set(path="/v1/subscriptions/" + find_uuid)
                  .add_query("replica", "aws"))

        # Forbidden delete request
        with override_email_config(DeploymentStage.NO_AUTH):
            self.assertDeleteResponse(url, requests.codes.forbidden, headers=self._get_auth_header())

        # Authorized delete
        self.assertDeleteResponse(url, requests.codes.okay, headers=self._get_auth_header())

        # 1. Check that previous delete worked
        # 2. Check that we can't delete files that don't exist
        self.assertDeleteResponse(url, requests.codes.not_found, headers=self._get_auth_header())

    def _put_subscription(self):
        url = str(UrlBuilder()
                  .set(path="/v1/subscriptions")
                  .add_query("replica", "aws"))
        resp_obj = self.assertPutResponse(
            url,
            requests.codes.created,
            json_request_body=dict(
                query=self.sample_percolate_query,
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


if __name__ == '__main__':
    unittest.main()
