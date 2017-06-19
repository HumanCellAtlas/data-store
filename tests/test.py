#!/usr/bin/env python
# coding: utf-8

from __future__ import absolute_import, division, print_function, unicode_literals

import os
import sys
import re
import json
import functools
import uuid
import unittest
import typing

import requests
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
            **kwargs) -> typing.Tuple[
                wrappers.Response,
                str,
                typing.Optional[dict]]:
        """
        Make a request given a HTTP method and a path.  The HTTP status code is
        checked against `expected_code`.

        If json_request_body is provided, it is serialized and set as the
        request body, and the content-type of the request is set to
        application/json.

        The first element of the return value is the response object.
        The second element of the return value is the response text.

        If `parse_response_as_json` is true, then attempt to parse the response
        body as JSON and return that as the third element of the return value.
        Otherwise, the third element of the return value is None.
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
        self.assertGetResponse("/v1/files", requests.codes.ok)

        self.assertHeadResponse(
            "/v1/files/91839244-66ab-408f-9be5-c82def201f26",
            requests.codes.ok)

        self.assertGetResponse(
            "/v1/files/91839244-66ab-408f-9be5-c82def201f26",
            requests.codes.bad_request)
        self.assertGetResponse(
            "/v1/files/91839244-66ab-408f-9be5-c82def201f26?replica=aws",
            requests.codes.found)

    def test_file_put(self):
        file_uuid = uuid.uuid4()
        response = self.assertPutResponse(
            "/v1/files/" + str(file_uuid),
            requests.codes.created,
            json_request_body=dict(
                source_url="s3://hca-dss-test-src/test_good_source_data",
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
        self.assertIn('timestamp', response[2])

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
