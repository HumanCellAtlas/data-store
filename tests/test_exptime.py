#!/usr/bin/env python
# coding: utf-8

import os
import sys
import unittest
import uuid

import requests

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.config import DeploymentStage
from tests.infra import DSSAssertMixin, DSSUploadMixin, ExpectedErrorFields, testmode
from tests.infra.server import ThreadedLocalServer


@testmode.standalone
class TestExptime(unittest.TestCase, DSSAssertMixin, DSSUploadMixin):
    @classmethod
    def setUpClass(cls):
        cls.app = ThreadedLocalServer()
        cls.app.start()
        cls.app._chalice_app._override_exptime_seconds = 15.0

    @classmethod
    def tearDownClass(cls):
        cls.app.shutdown()

    def test_exptime(self):
        self.assertGetResponse(
            "/internal/slow_request",
            requests.codes.gateway_timeout,
            expected_error=ExpectedErrorFields(
                code="timed_out",
                status=requests.codes.gateway_timeout,
            )
        )

    @unittest.skipIf(DeploymentStage.IS_PROD(), "Skipping synthetic 504 test for PROD.")
    def test_synthetic_504(self):
        file_uuid = str(uuid.uuid4())
        r = self.assertGetResponse(
            f"/v1/files/{file_uuid}?replica=aws",
            requests.codes.gateway_timeout,
            expected_error=ExpectedErrorFields(
                code="timed_out",
                status=requests.codes.gateway_timeout,
            ),
            headers={
                "DSS_FAKE_504_PROBABILITY": "1.0",
            }
        )
        with self.subTest('Retry-After headers are included in a GET /v1/bundles/{uuid} 504 response.'):
            self.assertEqual(int(r.response.headers['Retry-After']), 10)


if __name__ == '__main__':
    unittest.main()
