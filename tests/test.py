#!/usr/bin/env python
# coding: utf-8

from __future__ import absolute_import, division, print_function, unicode_literals

import functools
import json
import os
import re
import requests
import sys
import typing
import unittest
import uuid

from flask import wrappers

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, pkg_root)

import dss # noqa

class TestDSS(unittest.TestCase):
    def setUp(self):
        self.app = dss.create_app().app.test_client()
        self.sre = re.compile("^assert(.+)Response")

    def assertResponse(
            self,
            method: str,
            path: str,
            expected_code: int,
            json_request_body: typing.Optional[dict]=None,
            **kwargs) -> typing.Tuple[wrappers.Response, str, typing.Optional[dict]]:
        """
        Make a request given a HTTP method and a path.  The HTTP status code is checked against `expected_code`.

        If json_request_body is provided, it is serialized and set as the request body, and the content-type of the
        request is set to application/json.

        The first element of the return value is the response object.
        The second element of the return value is the response text.

        If `parse_response_as_json` is true, then attempt to parse the response body as JSON and return that as the
        third element of the return value.  Otherwise, the third element of the return value is None.
        """
        if json_request_body is not None:
            if 'data' in kwargs:
                self.fail("both json_input and data are defined")
            kwargs['data'] = json.dumps(json_request_body)
            kwargs['content_type'] = 'application/json'

        response = getattr(self.app, method)(path, **kwargs)
        self.assertEqual(response.status_code, expected_code)

        try:
            actual_json = json.loads(response.data.decode("utf-8"))
        except Exception:
            actual_json = None
        return response, response.data, actual_json

    def assertHeaders(
            self,
            response: wrappers.Response,
            expected_headers: dict = {}) -> None:
        for header_name, header_value in expected_headers.items():
            self.assertEqual(response.headers[header_name], header_value)

    # this allows for assert*Response, where * = the request method.
    def __getattr__(self, item: str) -> typing.Any:
        if item.startswith("assert"):
            mo = self.sre.match(item)
            if mo is not None:
                method = mo.group(1).lower()
                return functools.partial(self.assertResponse, method)

        if hasattr(super(TestDSS, self), '__getattr__'):
            return super(TestDSS, self).__getattr__(item)  # type: ignore
        else:
            raise AttributeError(item)

    def test_file_api(self):
        res = self.app.get("/v1/files")
        self.assertEqual(res.status_code, requests.codes.ok)

        res = self.app.head("/v1/files/123")
        self.assertEqual(res.status_code, requests.codes.ok)

        res = self.app.get("/v1/files/123")
        self.assertEqual(res.status_code, requests.codes.bad_request)
        res = self.app.get("/v1/files/123?replica=aws")
        self.assertEqual(res.status_code, requests.codes.found)

        res = self.app.put(
            '/v1/files/123',
            data=json.dumps(
                dict(source_url="s3://foobar",
                     bundle_uuid=str(uuid.uuid4()),
                     creator_uid=4321,
                     content_type="text/html",
                     )),
            content_type='application/json')
        self.assertEqual(res.status_code, requests.codes.created)
        self.assertEqual(res.headers['content-type'], "application/json")
        jsonres = json.loads(res.data.decode("utf-8"))
        self.assertIn('timestamp', jsonres)

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
