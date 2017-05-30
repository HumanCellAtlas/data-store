#!/usr/bin/env python
# coding: utf-8

from __future__ import absolute_import, division, print_function, unicode_literals

import os, sys, unittest, collections, json, datetime, glob
import requests

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, pkg_root)

import dss # noqa

class TestRequest:
    def call(self, method, path, json={}, headers={}, **kwargs):
        headers = [(k, v) for k, v in headers.items()]
        return self.app.open(path, method=method, headers=headers, data=json.dumps(json),
                             content_type="application/json", **kwargs)

class TestDSS(unittest.TestCase, TestRequest):
    def setUp(self):
        self.app = dss.create_app().app.test_client()

    def test_file_api(self):
        res = self.app.get("/v1/files")
        self.assertEqual(res.status_code, requests.codes.ok)

        res = self.app.head("/v1/files/123")
        self.assertEqual(res.status_code, requests.codes.ok)

        res = self.app.get("/v1/files/123")
        self.assertEqual(res.status_code, requests.codes.bad_request)
        res = self.app.get("/v1/files/123?replica=aws")
        self.assertEqual(res.status_code, requests.codes.found)

        print(self.app.post('/v1/files',
                            headers=[["x-header-1", "foo"], ["x-header-2", "bar"]],
                            data=json.dumps(dict(foo='bar')),
                            content_type='application/json',
                            query_string=dict(x="y")))

    def test_bundle_api(self):
        res = self.app.get("/v1/bundles")
        self.assertEqual(res.status_code, requests.codes.ok)
        res = self.app.get("/v1/bundles/123")
        self.assertEqual(res.status_code, requests.codes.ok)
        res = self.app.get("/v1/bundles/123/55555")
        self.assertEqual(res.status_code, requests.codes.bad_request)
        res = self.app.get("/v1/bundles/123/55555?replica=foo")
        self.assertEqual(res.status_code, requests.codes.ok)

if __name__ == '__main__':
    unittest.main()
