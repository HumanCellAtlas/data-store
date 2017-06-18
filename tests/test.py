#!/usr/bin/env python
# coding: utf-8

from __future__ import absolute_import, division, print_function, unicode_literals

import os
import sys
import re
import json
import functools
import uuid
import logging
import unittest
import typing

import boto3
import google.cloud.storage
import requests
from flask import wrappers

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, pkg_root)

import dss # noqa
from dss.events.handlers import sync # noqa

logging.basicConfig(level=logging.DEBUG)
for logger_name in logging.Logger.manager.loggerDict:  # type: ignore
    if logger_name.startswith("botocore") or logger_name.startswith("boto3.resources"):
        logging.getLogger(logger_name).setLevel(logging.WARNING)

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

class TestSyncUtils(unittest.TestCase):
    def test_sync_blob(self):
        gcs_bucket_name, s3_bucket_name = os.environ["DSS_GCS_TEST_BUCKET"], os.environ["DSS_S3_TEST_BUCKET"]
        logger = logging.getLogger(__name__)
        s3 = boto3.resource("s3")
        payload = os.urandom(2**20)
        test_key = "hca-dss-s3-to-gcs-sync-test"
        s3.Bucket(s3_bucket_name).Object(test_key).put(Body=payload)
        sync.sync_blob(source_platform="s3", source_key=test_key, dest_platform="gcs", logger=logger)
        # TODO: wait for GCSTS job and read back key

        test_key = "hca-dss-gcs-to-s3-sync-test"
        gcs_key_file = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
        gcs = google.cloud.storage.Client.from_service_account_json(gcs_key_file)
        gcs.bucket(gcs_bucket_name).blob(test_key).upload_from_string(payload)
        sync.sync_blob(source_platform="gcs", source_key=test_key, dest_platform="s3", logger=logger)
        dest_blob = s3.Bucket(s3_bucket_name).Object(test_key)
        self.assertEqual(dest_blob.get()["Body"].read(), payload)

if __name__ == '__main__':
    unittest.main()
