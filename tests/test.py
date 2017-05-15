#!/usr/bin/env python
# coding: utf-8

from __future__ import absolute_import, division, print_function, unicode_literals

import os, sys, unittest, collections, json, datetime, glob
import requests

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, pkg_root)

import dss # noqa

class TestDSS(unittest.TestCase):
    def setUp(self):
        self.app = dss.create_app().app.test_client()

    def test_dss_api(self):
        res = self.app.get("/v1/files")
        self.assertEqual(res.status_code, requests.codes.ok)
        res = self.app.get("/v1/files/123")
        self.assertEqual(res.status_code, requests.codes.bad_request)
        res = self.app.get("/v1/files/123?replica=foo")
        self.assertEqual(res.status_code, requests.codes.found)

if __name__ == '__main__':
    unittest.main()
