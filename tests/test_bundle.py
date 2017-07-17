#!/usr/bin/env python
# coding: utf-8

from __future__ import absolute_import, division, print_function, unicode_literals

import os
import sys
import unittest
import urllib
import uuid

import requests

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..')) # noqa
sys.path.insert(0, pkg_root) # noqa

import dss
from dss.config import BucketStage, override_bucket_config
from dss.util import UrlBuilder
from tests.infra import DSSAsserts, get_env


class TestDSS(unittest.TestCase, DSSAsserts):
    def setUp(self):
        DSSAsserts.setup(self)
        self.app = dss.create_app().app.test_client()
        dss.Config.set_config(dss.BucketStage.TEST)
        self.s3_test_fixtures_bucket = get_env("DSS_S3_BUCKET_TEST_FIXTURES")
        self.gs_test_fixtures_bucket = get_env("DSS_GS_BUCKET_TEST_FIXTURES")

    def test_bundle_get(self):
        self._test_bundle_get("aws")
        self._test_bundle_get("gcp")

    def _test_bundle_get(self, replica):
        bundle_uuid = "011c7340-9b3c-4d62-bf49-090d79daf198"
        version = "2017-06-20T214506.766634Z"

        url = str(UrlBuilder()
                  .set(path="/v1/bundles/" + bundle_uuid)
                  .add_query("replica", replica)
                  .add_query("version", version))

        with override_bucket_config(BucketStage.TEST_FIXTURE):
            response = self.assertGetResponse(
                url,
                requests.codes.ok)

        self.assertEqual(response[2]['bundle']['uuid'], bundle_uuid)
        self.assertEqual(response[2]['bundle']['version'], version)
        self.assertEqual(response[2]['bundle']['creator_uid'], 12345)
        self.assertEqual(response[2]['bundle']['files'][0]['content-type'], "text/plain")
        self.assertEqual(response[2]['bundle']['files'][0]['crc32c'], "e16e07b9")
        self.assertEqual(response[2]['bundle']['files'][0]['name'], "LICENSE")
        self.assertEqual(response[2]['bundle']['files'][0]['s3_etag'], "3b83ef96387f14655fc854ddc3c6bd57")
        self.assertEqual(response[2]['bundle']['files'][0]['sha1'], "2b8b815229aa8a61e483fb4ba0588b8b6c491890")
        self.assertEqual(response[2]['bundle']['files'][0]['sha256'],
                         "cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30")
        self.assertEqual(response[2]['bundle']['files'][0]['uuid'], "ce55fd51-7833-469b-be0b-5da88ebebfcd")
        self.assertEqual(response[2]['bundle']['files'][0]['version'], "2017-06-16T193604.240704Z")

    def test_bundle_put(self):
        self._test_bundle_put("aws", self.s3_test_fixtures_bucket)
        self._test_bundle_put("gcp", self.gs_test_fixtures_bucket)

    def _test_bundle_put(self, replica, fixtures_bucket):
        if replica == "aws":
            schema = "s3"
        elif replica == "gcp":
            schema = "gs"

        file_uuid = str(uuid.uuid4())
        bundle_uuid = str(uuid.uuid4())
        response = self.assertPutResponse(
            "/v1/files/" + file_uuid,
            requests.codes.created,
            json_request_body=dict(
                source_url=f"{schema}://{fixtures_bucket}/test_good_source_data/0",
                bundle_uuid=bundle_uuid,
                creator_uid=4321,
                content_type="text/html",
            ),
        )
        version = response[2]['version']

        url = str(UrlBuilder()
                  .set(path="/v1/bundles/" + bundle_uuid)
                  .add_query("replica", replica))

        response = self.assertPutResponse(
            url,
            requests.codes.created,
            json_request_body=dict(
                files=[
                    dict(
                        uuid=file_uuid,
                        version=version,
                        name="LICENSE",
                        indexed=False,
                    ),
                ],
                creator_uid=12345,
            ),
        )
        self.assertHeaders(
            response[0],
            {
                'content-type': "application/json",
            }
        )
        self.assertIn('version', response[2])


if __name__ == '__main__':
    unittest.main()
