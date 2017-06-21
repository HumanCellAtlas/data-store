#!/usr/bin/env python
# coding: utf-8

from __future__ import absolute_import, division, print_function, unicode_literals

import os
import sys
import unittest

import requests

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, pkg_root)

import dss  # noqa
from tests.infra import DSSAsserts  # noqa


class TestDSS(unittest.TestCase, DSSAsserts):
    def setUp(self):
        DSSAsserts.setup(self)
        self.app = dss.create_app().app.test_client()

    def test_bundle_api(self):
        self.assertGetResponse("/v1/bundles", requests.codes.ok)
        self.assertGetResponse(
            "/v1/bundles/91839244-66ab-408f-9be5-c82def201f26",
            requests.codes.ok)
        self.assertGetResponse(
            "/v1/bundles/91839244-66ab-408f-9be5-c82def201f26/55555",
            requests.codes.bad_request)
        self.assertGetResponse(
            "/v1/bundles/91839244-66ab-408f-9be5-c82def201f26/55555?replica=foo",
            requests.codes.ok)


if __name__ == '__main__':
    unittest.main()
