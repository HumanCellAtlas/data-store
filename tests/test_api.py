#!/usr/bin/env python
# coding: utf-8
"""
Functional Test of the API
"""
import datetime
import os
import sys
import unittest
import requests

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss.util import UrlBuilder
from dss.util.version import datetime_to_version_format
from dss.config import Replica, DeploymentStage, Config
from tests.infra import DSSAssertMixin, DSSUploadMixin, DSSStorageMixin, TestBundle, testmode, ExpectedErrorFields
from tests.infra.server import ThreadedLocalServer
from tests import get_auth_header


class TestApiErrors(unittest.TestCase, DSSAssertMixin):
    @classmethod
    def setUpClass(cls):
        cls.app = ThreadedLocalServer()
        cls.app.start()
        cls.app._chalice_app._override_exptime_seconds = 15.0

    @classmethod
    def tearDownClass(cls):
        cls.app.shutdown()

    @unittest.skipIf(DeploymentStage.IS_PROD(), "Skipping synthetic 504 test for PROD.")
    def test_504_post_bundle_HAS_NO_retry_after_response(self):
        """This is the only endpoint we care about NOT having a response with no Retry-After in the header."""
        uuid = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
        version = datetime_to_version_format(datetime.datetime.utcnow())

        url = str(UrlBuilder().set(path=f"/v1/bundles/{uuid}/checkout")
                  .add_query("version", version)
                  .add_query("replica", 'aws'))

        r = self.assertPostResponse(
            url,
            504,
            expected_error=ExpectedErrorFields(
                code="timed_out",
                status=requests.codes.gateway_timeout,
            ),
            headers={"DSS_FAKE_504_PROBABILITY": "1.0"}
        )
        self.assertTrue('Retry-After' not in r.response.headers)

    def test_500_get_bundle_HAS_retry_after_response(self):
        """All endpoints except POST /bundles/{uuid}/checkout should have a Retry-After header for 500 errors."""
        res = self.get_bundle_response(code=requests.codes.server_error, code_alias='unhandled_exception')
        self.assertEqual(int(res.response.headers['Retry-After']), 10)

    def test_502_get_bundle_HAS_retry_after_response(self):
        """All endpoints except POST /bundles/{uuid}/checkout should have a Retry-After header for 502 errors."""
        res = self.get_bundle_response(code=requests.codes.bad_gateway, code_alias='bad_gateway')
        self.assertEqual(int(res.response.headers['Retry-After']), 10)

    def test_503_get_bundle_HAS_retry_after_response(self):
        """All endpoints except POST /bundles/{uuid}/checkout should have a Retry-After header for 503 errors."""
        res = self.get_bundle_response(code=requests.codes.service_unavailable, code_alias='service_unavailable')
        self.assertEqual(int(res.response.headers['Retry-After']), 10)

    @unittest.skipIf(DeploymentStage.IS_PROD(), "Skipping synthetic 504 test for PROD.")
    def test_504_get_bundle_HAS_retry_after_response(self):
        """All endpoints except POST /bundles/{uuid}/checkout should have a Retry-After header for 504 errors."""
        res = self.get_bundle_response(code=requests.codes.gateway_timeout, code_alias='timed_out')
        self.assertEqual(int(res.response.headers['Retry-After']), 10)

    def get_bundle_response(self, code, code_alias):
        uuid = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
        version = datetime_to_version_format(datetime.datetime.utcnow())

        url = str(UrlBuilder().set(path=f"/v1/bundles/{uuid}")
                  .add_query("version", version)
                  .add_query("replica", 'aws'))

        response = self.assertGetResponse(
            url,
            code,
            expected_error=ExpectedErrorFields(
                code=code_alias,
                status=code,
            ),
            headers={f'DSS_FAKE_{code}_PROBABILITY': '1.0'}
        )
        return response


@testmode.integration
class TestApi(unittest.TestCase, DSSAssertMixin, DSSUploadMixin, DSSStorageMixin):
    @classmethod
    def setUpClass(cls):
        os.environ['DSS_VERSION'] = "test_version"
        cls.app = ThreadedLocalServer()
        cls.app.start()

    @classmethod
    def tearDownClass(cls):
        cls.app.shutdown()

    def setUp(self):
        self.replica = Replica.aws
        dss.Config.set_config(dss.BucketConfig.TEST)
        self.blobstore = dss.Config.get_blobstore_handle(self.replica)
        self.bucket = self.replica.bucket

    BUNDLE_FIXTURE = "fixtures/example_bundle"

    def test_creation_and_retrieval_of_files_and_bundle(self):

        # FIXME: This test doesn't do much because it uses the test bucket which lacks the fixtures for the test bundle.
        # FIMXE: In particular it does not test any of the /files routes. (hannes)

        """
        Test file and bundle lifecycle.
        Exercises:
          - PUT /files/<uuid>
          - PUT /bundles/<uuid>
          - GET /bundles/<uuid>
          - GET /files/<uuid>
        and checks that data corresponds where appropriate.
        """
        bundle = TestBundle(self.blobstore, self.BUNDLE_FIXTURE, self.bucket, self.replica)
        self.upload_files_and_create_bundle(bundle, self.replica)
        self.get_bundle_and_check_files(bundle, self.replica)

    def test_get_version(self):
        """
        Test /version endpoint and configuration
        """
        res = self.assertGetResponse("/version", requests.codes.ok, headers=get_auth_header())
        self.assertEquals(res.json['version_info']['version'], os.environ['DSS_VERSION'])

    def test_read_only(self):
        uuid = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
        body = dict(
            files=[],
            creator_uid=12345,
            es_query={},
            callback_url="https://take.me.to.funkytown",
            source_url="s3://urlurlurlbaby",
            description="supercalifragilisticexpialidocious",
            details={},
            reason="none",
            contents=[],
            name="frank",
        )
        version = datetime_to_version_format(datetime.datetime.utcnow())
        tests = [(path, replica)
                 for path in ("bundles", "files", "subscriptions", "collections")
                 for replica in ("aws", "gcp")]

        os.environ['DSS_READ_ONLY_MODE'] = "True"
        try:
            for path, replica in tests:
                with self.subTest(path=path, replica=replica):
                    put_url = str(UrlBuilder().set(path=f"/v1/{path}/{uuid}")
                                  .add_query("version", version)
                                  .add_query("replica", replica))
                    delete_url = str(UrlBuilder().set(path=f"/v1/{path}/{uuid}")
                                     .add_query("version", "asdf")
                                     .add_query("replica", replica))
                    json_request_body = body.copy()

                    if path == "subscriptions":
                        put_url = str(UrlBuilder().set(path=f"/v1/{path}")
                                      .add_query("version", "asdf")
                                      .add_query("uuid", uuid)
                                      .add_query("replica", replica))
                        delete_url = None
                        del json_request_body['source_url']
                        del json_request_body['creator_uid']
                        del json_request_body['description']
                        del json_request_body['reason']
                        del json_request_body['details']
                        del json_request_body['files']
                        del json_request_body['contents']
                        del json_request_body['name']
                    elif path == "files":
                        delete_url = None
                    elif path == "collections":
                        put_url = str(UrlBuilder().set(path=f"/v1/{path}")
                                      .add_query("version", version)
                                      .add_query("uuid", uuid)
                                      .add_query("replica", replica))
                        delete_url = str(UrlBuilder().set(path=f"/v1/{path}/{uuid}")
                                         .add_query("version", "asdf")
                                         .add_query("replica", replica))
                        del json_request_body['es_query']
                        del json_request_body['callback_url']

                    if put_url:
                        self.assertPutResponse(
                            put_url,
                            requests.codes.unavailable,
                            json_request_body=json_request_body,
                            headers=get_auth_header()
                        )

                    if delete_url:
                        self.assertDeleteResponse(
                            delete_url,
                            requests.codes.unavailable,
                            json_request_body=json_request_body,
                            headers=get_auth_header()
                        )
        finally:
            del os.environ['DSS_READ_ONLY_MODE']


if __name__ == '__main__':
    unittest.main()
