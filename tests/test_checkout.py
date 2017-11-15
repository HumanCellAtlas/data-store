#!/usr/bin/env python
# coding: utf-8

import os
import sys
import unittest

import requests

from dss.config import override_bucket_config, BucketConfig
from dss.util import UrlBuilder
from dss.util.checkout import get_manifest_files

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from tests.infra import DSSAssertMixin, DSSUploadMixin, get_env
from tests.infra.server import ThreadedLocalServer


class TestFileApi(unittest.TestCase, DSSAssertMixin, DSSUploadMixin):
    @classmethod
    def setUpClass(cls):
        cls.app = ThreadedLocalServer()
        cls.app.start()

    @classmethod
    def tearDownClass(cls):
        cls.app.shutdown()

    def setUp(self):
        dss.Config.set_config(dss.BucketConfig.TEST)
        self.s3_test_fixtures_bucket = get_env("DSS_S3_BUCKET_TEST_FIXTURES")
        self.s3_test_bucket = get_env("DSS_S3_BUCKET_TEST")

    def test_sanity_check_valid(self):
        self.launch_checkout()

    def test_pre_execution_check_doesnt_exist(self):
        replica = "aws"
        non_existent_bundle_uuid = "011c7340-9b3c-4d62-bf49-090d79daf111"
        version = "2017-06-20T214506.766634Z"
        request_body = {"destination": self.s3_test_bucket, "email": "rkisin@chanzuckerberg.com"}

        url = str(UrlBuilder()
                  .set(path="/v1/bundles/" + non_existent_bundle_uuid + "/checkout")
                  .add_query("replica", replica)
                  .add_query("version", version))

        with override_bucket_config(BucketConfig.TEST_FIXTURE):
            resp_obj = self.assertPostResponse(
                url,
                requests.codes.bad_request,
                request_body
            )
        self.assertEqual(resp_obj.json['code'], 'not_found')

    def test_sanity_check_no_replica(self):
        bundle_uuid = "011c7340-9b3c-4d62-bf49-090d79daf198"
        version = "2017-06-20T214506.766634Z"
        request_body = {"destination": self.s3_test_bucket, "email": "rkisin@chanzuckerberg.com"}

        url = str(UrlBuilder()
                  .set(path="/v1/bundles/" + bundle_uuid + "/checkout")
                  .add_query("replica", "")
                  .add_query("version", version))

        with override_bucket_config(BucketConfig.TEST_FIXTURE):
            self.assertPostResponse(
                url,
                requests.codes.bad_request,
                request_body
            )

    def launch_checkout(self) -> str:
        replica = "aws"
        bundle_uuid = "011c7340-9b3c-4d62-bf49-090d79daf198"
        version = "2017-06-20T214506.766634Z"
        request_body = {"destination": self.s3_test_bucket, "email": "rkisin@chanzuckerberg.com"}

        url = str(UrlBuilder()
                  .set(path="/v1/bundles/" + bundle_uuid + "/checkout")
                  .add_query("replica", replica)
                  .add_query("version", version))

        with override_bucket_config(BucketConfig.TEST_FIXTURE):
            resp_obj = self.assertPostResponse(
                url,
                requests.codes.ok,
                request_body
            )
        execution_arn = resp_obj.json["checkout_job_id"]
        self.assertIsNotNone(execution_arn)

        return execution_arn

    def test_status(self):
        exec_arn = self.launch_checkout()
        url = str(UrlBuilder().set(path="/v1/bundles/checkout/" + exec_arn))
        with override_bucket_config(BucketConfig.TEST_FIXTURE):
            resp_obj = self.assertGetResponse(
                url,
                requests.codes.ok
            )
        status = resp_obj.json.get('status')
        self.assertIsNotNone(status)
        self.assertIn(status, ['RUNNING', 'SUCCEEDED'])

    def test_manifest_files(self):
        bundle_uuid = "011c7340-9b3c-4d62-bf49-090d79daf198"
        version = "2017-06-20T214506.766634Z"
        replica = "aws"
        file_count = 0
        with override_bucket_config(BucketConfig.TEST_FIXTURE):
            for file in get_manifest_files(bundle_uuid, version, replica):
                file_count += 1
        self.assertEqual(file_count, 1)
