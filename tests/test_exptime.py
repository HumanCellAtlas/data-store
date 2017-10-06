#!/usr/bin/env python
# coding: utf-8

import os
import sys
import unittest

import requests

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests.infra import DSSAssertMixin, DSSUploadMixin, ExpectedErrorFields
from tests.infra.server import ThreadedLocalServer


class TestExptime(unittest.TestCase, DSSAssertMixin, DSSUploadMixin):
    @classmethod
    def setUpClass(cls):
        cls.app = ThreadedLocalServer()
        cls.app.start()

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

if __name__ == '__main__':
    unittest.main()
