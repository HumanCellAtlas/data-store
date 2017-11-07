#!/usr/bin/env python
# coding: utf-8

import sys
import unittest
import requests


import os

from dss.config import override_bucket_config, BucketConfig
from dss.util import UrlBuilder

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
        self.gs_test_fixtures_bucket = get_env("DSS_GS_BUCKET_TEST_FIXTURES")
        self.s3_test_bucket = get_env("DSS_S3_BUCKET_TEST")
        self.gs_test_bucket = get_env("DSS_GS_BUCKET_TEST")

    def test_sanity_check(self, replica):
        bundle_uuid = "011c7340-9b3c-4d62-bf49-090d79daf198"
        version = "2017-06-20T214506.766634Z"
        request_body = {"destination": self.s3_test_bucket, "email": "rkisin@chanzuckerberg.com"}

        url = str(UrlBuilder()
                  .set(path="/v1/bundles/" + bundle_uuid + "/checkout")
                  .add_query("replica", replica)
                  .add_query("version", version))

        with override_bucket_config(BucketConfig.TEST_FIXTURE):
            resp_obj = self.assertGetResponse(
                url,
                requests.codes.ok,
                request_body
            )
        self.assertIsNotNone(resp_obj["execution_id"])
