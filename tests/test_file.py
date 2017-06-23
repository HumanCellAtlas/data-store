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

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, pkg_root)

import dss  # noqa
from dss.config import override_s3_config  # noqa
from tests.infra import DSSAsserts, UrlBuilder  # noqa


class TestFileApi(unittest.TestCase, DSSAsserts):
    def setUp(self):
        DSSAsserts.setup(self)
        self.app = dss.create_app().app.test_client()

    def test_file_put(self):
        file_uuid = uuid.uuid4()
        response = self.assertPutResponse(
            "/v1/files/" + str(file_uuid),
            requests.codes.created,
            json_request_body=dict(
                source_url="s3://hca-dss-test-src/test_good_source_data/0",
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
        file_uuid = uuid.uuid4()
        response = self.assertPutResponse(
            "/v1/files/" + str(file_uuid),
            requests.codes.created,
            json_request_body=dict(
                source_url="s3://hca-dss-test-src/test_good_source_data/incorrect_case_checksum",
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
        file_uuid = "ce55fd51-7833-469b-be0b-5da88ebebfcd"
        version = "2017-06-16T19:36:04.240704Z"

        url = str(UrlBuilder()
                  .set(path="/v1/files/" + file_uuid)
                  .add_query("replica", "aws")
                  .add_query("version", version))

        with override_s3_config("hca-dss-test-src"):
            self.assertHeadResponse(
                url,
                requests.codes.ok
            )

            # TODO: (ttung) verify headers

    def test_file_get_specific(self):
        """
        Verify we can successfully fetch a specific file UUID+version.
        """
        file_uuid = "ce55fd51-7833-469b-be0b-5da88ebebfcd"
        version = "2017-06-16T19:36:04.240704Z"

        url = str(UrlBuilder()
                  .set(path="/v1/files/" + file_uuid)
                  .add_query("replica", "aws")
                  .add_query("version", version))

        with override_s3_config("hca-dss-test-src"):
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
        """
        Verify we can successfully fetch the latest version of a file UUID.
        """
        file_uuid = "ce55fd51-7833-469b-be0b-5da88ebebfcd"

        url = str(UrlBuilder()
                  .set(path="/v1/files/" + file_uuid)
                  .add_query("replica", "aws"))

        with override_s3_config("hca-dss-test-src"):
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
