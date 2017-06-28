#!/usr/bin/env python
# coding: utf-8

from __future__ import absolute_import, division, print_function, unicode_literals

import hashlib
import os
import sys
import unittest
import urllib
import uuid

import requests

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..')) # noqa
sys.path.insert(0, pkg_root) # noqa

import dss
from dss.config import BucketConfig, override_bucket_config
from tests.infra import DSSAsserts, UrlBuilder


class TestFileApi(unittest.TestCase, DSSAsserts):
    def setUp(self):
        DSSAsserts.setup(self)
        self.app = dss.create_app().app.test_client()

    def test_file_put(self):
        self._test_file_put("s3")
        self._test_file_put("gs")

    def _test_file_put(self, scheme):
        file_uuid = uuid.uuid4()
        response = self.assertPutResponse(
            "/v1/files/" + str(file_uuid),
            requests.codes.created,
            json_request_body=dict(
                source_url=scheme + "://hca-dss-test-src/test_good_source_data/0",
                bundle_uuid=str(uuid.uuid4()),
                creator_uid=4321,
                content_type="text/html",
            ),
        )
        self.assertHeaders(
            response[0],
            {
                'content-type': "application/json",
            }
        )
        self.assertIn('version', response[2])

    # This is a test specific to AWS since it has separate notion of metadata and tags.
    def test_file_put_metadata_from_tags(self):
        file_uuid = uuid.uuid4()
        response = self.assertPutResponse(
            "/v1/files/" + str(file_uuid),
            requests.codes.created,
            json_request_body=dict(
                source_url="s3://hca-dss-test-src/test_good_source_data/metadata_in_tags",
                bundle_uuid=str(uuid.uuid4()),
                creator_uid=4321,
                content_type="text/html",
            ),
        )
        self.assertHeaders(
            response[0],
            {
                'content-type': "application/json",
            }
        )
        self.assertIn('version', response[2])

    def test_file_put_upper_case_checksums(self):
        self._test_file_put_upper_case_checksums("s3")
        self._test_file_put_upper_case_checksums("gs")

    def _test_file_put_upper_case_checksums(self, scheme):
        file_uuid = uuid.uuid4()
        response = self.assertPutResponse(
            "/v1/files/" + str(file_uuid),
            requests.codes.created,
            json_request_body=dict(
                source_url=scheme + "://hca-dss-test-src/test_good_source_data/incorrect_case_checksum",
                bundle_uuid=str(uuid.uuid4()),
                creator_uid=4321,
                content_type="text/html",
            ),
        )
        self.assertHeaders(
            response[0],
            {
                'content-type': "application/json",
            }
        )
        self.assertIn('version', response[2])

    def test_file_head(self):
        self._test_file_head("aws")
        self._test_file_head("gcp")

    def _test_file_head(self, replica):
        file_uuid = "ce55fd51-7833-469b-be0b-5da88ebebfcd"
        version = "2017-06-16T19:36:04.240704Z"

        url = str(UrlBuilder()
                  .set(path="/v1/files/" + file_uuid)
                  .add_query("replica", replica)
                  .add_query("version", version))

        with override_bucket_config(BucketConfig.TEST_FIXTURE):
            self.assertHeadResponse(
                url,
                requests.codes.ok
            )

            # TODO: (ttung) verify headers

    def test_file_get_specific(self):
        self._test_file_get_specific("aws")
        self._test_file_get_specific("gcp")

    def _test_file_get_specific(self, replica):
        """
        Verify we can successfully fetch a specific file UUID+version.
        """
        file_uuid = "ce55fd51-7833-469b-be0b-5da88ebebfcd"
        version = "2017-06-16T19:36:04.240704Z"

        url = str(UrlBuilder()
                  .set(path="/v1/files/" + file_uuid)
                  .add_query("replica", replica)
                  .add_query("version", version))

        with override_bucket_config(BucketConfig.TEST_FIXTURE):
            response = self.assertGetResponse(
                url,
                requests.codes.found
            )

            url = response[0].headers['Location']
            sha1 = response[0].headers['X-DSS-SHA1']
            data = requests.get(url)
            self.assertEqual(len(data.content), 11358)

            # verify that the downloaded data matches the stated checksum
            hasher = hashlib.sha1()
            hasher.update(data.content)
            self.assertEqual(hasher.hexdigest(), sha1)

            # TODO: (ttung) verify more of the headers

    def test_file_get_latest(self):
        self._test_file_get_latest("aws")
        self._test_file_get_latest("gcp")

    def _test_file_get_latest(self, replica):
        """
        Verify we can successfully fetch the latest version of a file UUID.
        """
        file_uuid = "ce55fd51-7833-469b-be0b-5da88ebebfcd"

        url = str(UrlBuilder()
                  .set(path="/v1/files/" + file_uuid)
                  .add_query("replica", replica))

        with override_bucket_config(BucketConfig.TEST_FIXTURE):
            response = self.assertGetResponse(
                url,
                requests.codes.found
            )

            url = response[0].headers['Location']
            sha1 = response[0].headers['X-DSS-SHA1']
            data = requests.get(url)
            self.assertEqual(len(data.content), 8685)

            # verify that the downloaded data matches the stated checksum
            hasher = hashlib.sha1()
            hasher.update(data.content)
            self.assertEqual(hasher.hexdigest(), sha1)

            # TODO: (ttung) verify more of the headers


if __name__ == '__main__':
    unittest.main()
