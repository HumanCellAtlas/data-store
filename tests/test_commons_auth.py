#!/usr/bin/env python
# coding: utf-8
import json
import logging
import sys
import unittest
import uuid
from contextlib import contextmanager

import boto3
import connexion.apis.abstract
import os
import requests

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss.logging import configure_test_logging
from dss.util import UrlBuilder
from tests import get_auth_header
from tests.infra.server import ThreadedLocalServer
from dss.config import Replica
from tests.infra import DSSAssertMixin, DSSUploadMixin, get_env, testmode

logger = logging.getLogger(__name__)


def setUpModule():
    configure_test_logging()


"""
This test verifies that all APIs respond with 'unauthorized'
responses if valid authentication is not provided.

An authentication token is added implicitly and by default to
assertResponse.  All other standard DSS tests are run using
that authentication token.

NOTE: This test suite could be made more comprehensive and more
resilient to API changes by loading the swagger spec and using
that to identify and test that all API methods require valid
authentication.
"""

client = boto3.client('secretsmanager')


@testmode.standalone
class TestCommonsAuth(unittest.TestCase, DSSAssertMixin, DSSUploadMixin):
    @classmethod
    def setUpClass(cls):
        # we mask the whitelist with a test whitelist that is empty.
        cls._saved_whitelist_name = os.environ['EMAIL_WHITELIST_NAME']
        cls.whitelist_name = f'datastore/whitelist/test/{str(uuid.uuid4())}'
        os.environ['EMAIL_WHITELIST_NAME'] = cls.whitelist_name
        client.create_secret(
            Name=cls.whitelist_name,
            Description='test secret for data store whitelist. If this has been lying around '
                        'for any reasonable length of time then test cleanup is '
                        'probably not working properly',
            SecretString=json.dumps({'email': ''}),
        )
        # The environment var needs to be set before the next line in order to be picked up in time
        cls.app = ThreadedLocalServer()
        cls.app.start()

        cls.s3_test_fixtures_bucket = get_env('DSS_S3_BUCKET_TEST_FIXTURES')
        cls.gs_test_fixtures_bucket = get_env('DSS_GS_BUCKET_TEST_FIXTURES')

        file_uuid = 'ce55fd51-7833-469b-be0b-5da88ebebfcd'
        bundle_uuid = '011c7340-9b3c-4d62-bf49-090d79daf198'
        arn = '49e22e42-27eb-4803-9df1-51e781f82174'

        cls.search_url = f'/v1/search'
        cls.file_url = f'/v1/files/{file_uuid}'
        cls.bundle_url = f'/v1/bundles/{bundle_uuid}'
        cls.bundle_get_checkout_url = f'/v1/bundles/checkout/{arn}'
        cls.bundle_post_checkout_url = f'/v1/bundles/{arn}/checkout'

    @classmethod
    def tearDownClass(cls):
        cls.app.shutdown()
        os.environ['EMAIL_WHITELIST_NAME'] = cls._saved_whitelist_name
        client.delete_secret(SecretId=cls.whitelist_name)

    def construct_url(self, path: str, replica: Replica) -> str:
        version = '2017-06-20T214506.766634Z'
        return str(UrlBuilder()
                   .set(path=path)
                   .add_query('replica', replica.name)
                   .add_query('version', version))

    def setUp(self):
        dss.Config.set_config(dss.BucketConfig.TEST)

    def test_get_bundle_checkout_auth_errors(self):
        self.aws_gcp_auth(self.bundle_get_checkout_url, calltype='get')

    def test_post_bundle_checkout_auth_errors(self):
        self.aws_gcp_auth(self.bundle_post_checkout_url, calltype='post')

    def test_get_file_auth_errors(self):
        self.aws_gcp_auth(self.file_url, calltype='get')

    def test_put_file_auth_errors(self):
        self.aws_gcp_auth(self.file_url, calltype='put')

    def test_head_file_auth_errors(self):
        self.aws_gcp_auth(self.file_url, calltype='head')

    def test_get_bundle_auth_errors(self):
        self.aws_gcp_auth(self.bundle_url, calltype='get')

    def test_put_bundle_auth_errors(self):
        self.aws_gcp_auth(self.bundle_url, calltype='put')

    def test_delete_bundle_auth_errors(self):
        self.aws_gcp_auth(self.bundle_url, calltype='delete')

    def test_post_search_auth_errors(self):
        self.aws_gcp_auth(self.search_url, calltype='post')

    def aws_gcp_auth(self, url, calltype):
        """Run aws first, then gcp."""
        self.run_auth_errors(self.construct_url(url, Replica.aws), calltype)
        self.run_auth_errors(self.construct_url(url, Replica.gcp), calltype)

    def run_auth_errors(self, url, calltype):
        if calltype == 'get':
            response = self.assertGetResponse
        elif calltype == 'put':
            response = self.assertPutResponse
        elif calltype == 'delete':
            response = self.assertDeleteResponse
        elif calltype == 'head':
            response = self.assertHeadResponse
        elif calltype == 'post':
            response = self.assertPostResponse
        else:
            raise ValueError('Unexpected/unsupported CallType: ' + calltype)

        error_key = 'Content-Type'
        error_value = 'application/problem+json'

        # the email is not registered in the whitelist since we are masking the environment variable
        # for the whitelist during this test
        resp_obj = response(url, requests.codes.forbidden, headers=get_auth_header())
        self.assertEqual(resp_obj.response.headers[error_key], error_value)
        if calltype != 'head':
            self.assertEqual(resp_obj.json['title'], "User is not authorized to access this resource")

        # Gibberish auth header
        expected_code = requests.codes.unauthorized
        resp_obj = response(url, expected_code, headers=get_auth_header(False))
        self.assertEqual(resp_obj.response.headers[error_key], error_value)
        # Check title message to verify oauth is rejecting
        if calltype != 'head':
            self.assertEqual(resp_obj.json['title'], "Provided oauth token is not valid")

        # No auth header
        try:
            DSSAssertMixin.include_auth_header = False
            response(url, expected_code)
        finally:
            DSSAssertMixin.include_auth_header = True

    @contextmanager
    def throw_403(self):
        orig_testing_403 = connexion.apis.abstract.Operation.testing_403
        try:
            connexion.apis.abstract.Operation.testing_403 = True
            yield
        finally:
            connexion.apis.abstract.Operation.testing_403 = orig_testing_403


if __name__ == '__main__':
    unittest.main()
