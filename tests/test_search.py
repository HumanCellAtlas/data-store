#!/usr/bin/env python
# coding: utf-8

from __future__ import absolute_import, division, print_function, unicode_literals

import json
import os
import sys
import unittest

import requests

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, pkg_root)

import dss  # noqa
from tests.infra import DSSAsserts  # noqa


class TestSearch(unittest.TestCase, DSSAsserts):
    def setUp(self):
        DSSAsserts.setup(self)
        self.app = dss.create_app().app.test_client()

    def test_search(self):
        self.assertGetResponse(
            '/v1/search',
            query_string=dict(query=json.dumps(dict(foo=1))),
            expected_code=requests.codes.ok)
