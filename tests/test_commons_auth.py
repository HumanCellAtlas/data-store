#!/usr/bin/env python
# coding: utf-8

import logging
import sys
import unittest
from contextlib import contextmanager

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
from dss.config import override_bucket_config, BucketConfig, Replica
from tests.infra import DSSAssertMixin, DSSUploadMixin, get_env, testmode

logger = logging.getLogger(__name__)


def setUpModule():
    configure_test_logging()


@testmode.standalone
class TestCommonsAuth(unittest.TestCase, DSSAssertMixin, DSSUploadMixin):
    @classmethod
    def setUpClass(cls):
        cls.app = ThreadedLocalServer()
        cls.app.start()

    @classmethod
    def tearDownClass(cls):
        cls.app.shutdown()

    def construct_url(self, path: str, replica: Replica) -> str:
        version = '2017-06-20T214506.766634Z'
        return str(UrlBuilder()
                   .set(path=path)
                   .add_query('replica', replica.name)
                   .add_query('version', version))

    def launch_checkout(self, url: str, replica: Replica) -> str:
        with override_bucket_config(BucketConfig.TEST_FIXTURE):
            resp_obj = self.assertPostResponse(
                url,
                requests.codes.ok,
                {'destination': replica.checkout_bucket})
        return resp_obj.json['checkout_job_id']

    def setUp(self):
        dss.Config.set_config(dss.BucketConfig.TEST)
        self.s3_test_fixtures_bucket = get_env('DSS_S3_BUCKET_TEST_FIXTURES')
        self.gs_test_fixtures_bucket = get_env('DSS_GS_BUCKET_TEST_FIXTURES')

        file_uuid = 'ce55fd51-7833-469b-be0b-5da88ebebfcd'
        bundle_uuid = '011c7340-9b3c-4d62-bf49-090d79daf198'

        # search API urls
        self.aws_search_url = self.construct_url(f'/v1/search/{file_uuid}', Replica.aws)
        self.gcp_search_url = self.construct_url(f'/v1/search/{file_uuid}', Replica.gcp)
        # files API urls
        self.aws_file_url = self.construct_url(f'/v1/files/{file_uuid}', Replica.aws)
        self.gcp_file_url = self.construct_url(f'/v1/files/{file_uuid}', Replica.gcp)
        # bundles API urls
        self.aws_bundle_url = self.construct_url(f'/v1/bundles/{bundle_uuid}', Replica.aws)
        self.gcp_bundle_url = self.construct_url(f'/v1/bundles/{bundle_uuid}', Replica.gcp)
        self.aws_bundle_post_checkout_url = self.construct_url(f'/v1/bundles/{bundle_uuid}/checkout', Replica.aws)
        self.gcp_bundle_post_checkout_url = self.construct_url(f'/v1/bundles/{bundle_uuid}/checkout', Replica.gcp)
        aws_arn = self.launch_checkout(self.aws_bundle_post_checkout_url, Replica.aws)
        self.aws_bundle_get_checkout_url = self.construct_url(f'/v1/bundles/checkout/{aws_arn}', Replica.aws)
        gcp_arn = self.launch_checkout(self.gcp_bundle_post_checkout_url, Replica.gcp)
        self.gcp_bundle_get_checkout_url = self.construct_url(f'/v1/bundles/checkout/{gcp_arn}', Replica.gcp)

    def test_get_bundle_checkout_auth_errors(self):
        self.run_auth_errors(self.aws_bundle_get_checkout_url, 'get')
        self.run_auth_errors(self.gcp_bundle_get_checkout_url, 'get')

    def test_post_bundle_checkout_auth_errors(self):
        self.run_auth_errors(self.aws_bundle_post_checkout_url, 'post')
        self.run_auth_errors(self.gcp_bundle_post_checkout_url, 'post')

    def test_get_file_auth_errors(self):
        self.run_auth_errors(self.aws_file_url, 'get')
        self.run_auth_errors(self.gcp_file_url, 'get')

    def test_put_file_auth_errors(self):
        self.run_auth_errors(self.aws_file_url, 'put')
        self.run_auth_errors(self.gcp_file_url, 'put')

    def test_head_file_auth_errors(self):
        self.run_auth_errors(self.aws_file_url, 'head')
        self.run_auth_errors(self.gcp_file_url, 'head')

    def test_get_bundle_auth_errors(self):
        self.run_auth_errors(self.aws_bundle_url, 'get')
        self.run_auth_errors(self.gcp_bundle_url, 'get')

    def test_put_bundle_auth_errors(self):
        self.run_auth_errors(self.aws_bundle_url, 'put')
        self.run_auth_errors(self.gcp_bundle_url, 'put')

    def test_delete_bundle_auth_errors(self):
        self.run_auth_errors(self.aws_bundle_url, 'delete')
        self.run_auth_errors(self.gcp_bundle_url, 'delete')

    def test_post_search_auth_errors(self):
        self.run_auth_errors(self.aws_search_url, 'post',
                             error_key='x-amzn-ErrorType',
                             error_value='UnauthorizedException',
                             post_search=True)
        self.run_auth_errors(self.gcp_search_url, 'post',
                             error_key='x-amzn-ErrorType',
                             error_value='UnauthorizedException',
                             post_search=True)

    def run_auth_errors(self, url, calltype,
                        error_key='Content-Type',
                        error_value='application/problem+json',
                        post_search=False):
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

        # Unauthorized email
        with self.throw_403():
            resp_obj = response(url, requests.codes.forbidden, headers=get_auth_header())
            self.assertEqual(resp_obj.response.headers[error_key], error_value)

        # Gibberish auth header
        expected_code = requests.codes.unauthorized if not post_search else requests.codes.forbidden
        resp_obj = response(url, expected_code, headers=get_auth_header(False))
        self.assertEqual(resp_obj.response.headers[error_key], error_value)

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
