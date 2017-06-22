#!/usr/bin/env python
# coding: utf-8

from __future__ import absolute_import, division, print_function, unicode_literals

import os
import sys
import unittest
import urllib
import uuid

import requests

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, pkg_root)

import dss  # noqa
from tests.infra import DSSAsserts  # noqa


class TestDSS(unittest.TestCase, DSSAsserts):
    def setUp(self):
        DSSAsserts.setup(self)
        self.app = dss.create_app().app.test_client()

    def test_bundle_get(self):
        bundle_uuid = "ce55fd51-7833-469b-be0b-5da88ebebfcd"
        version = "2017-06-16T19:36:04.240704Z"

        url = urllib.parse.urlunparse((
            "",
            "",
            "/v1/bundles/" + bundle_uuid,
            "",
            urllib.parse.urlencode(
                (("replica", "aws"),
                 ("version", version)),
                doseq=True,
            ),
            "",
        ))

        self.assertGetResponse(
            url,
            requests.codes.ok)

    def test_bundle_put(self):
        file_uuid = uuid.uuid4()
        bundle_uuid = uuid.uuid4()
        response = self.assertPutResponse(
            "/v1/files/" + str(file_uuid),
            requests.codes.created,
            json_request_body=dict(
                source_url="s3://hca-dss-test-src/test_good_source_data/0",
                bundle_uuid=str(bundle_uuid),
                creator_uid=4321,
                content_type="text/html",
            ),
        )
        version = response[2]['version']

        response = self.assertPutResponse(
            '/v1/bundles/{}?replica=aws'.format(bundle_uuid),
            requests.codes.created,
            json_request_body=dict(
                files=[
                    dict(
                        uuid=str(file_uuid),
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
