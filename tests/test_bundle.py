#!/usr/bin/env python
# coding: utf-8

import datetime
import hashlib
import os
import sys
import typing
import unittest
import urllib.parse
import uuid

import requests

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss.config import DeploymentStage, Config, override_bucket_config
from dss.util import UrlBuilder
from tests.infra import DSSAssertMixin, DSSUploadMixin, ExpectedErrorFields, get_env
from tests.infra.server import ThreadedLocalServer


class TestDSS(unittest.TestCase, DSSAssertMixin, DSSUploadMixin):
    @classmethod
    def setUpClass(cls):
        cls.app = ThreadedLocalServer()
        cls.app.start()

    @classmethod
    def tearDownClass(cls):
        cls.app.shutdown()

    def setUp(self):
        dss.Config.set_config(dss.DeploymentStage.TEST)
        self.s3_test_fixtures_bucket = get_env("DSS_S3_BUCKET_TEST_FIXTURES")
        self.gs_test_fixtures_bucket = get_env("DSS_GS_BUCKET_TEST_FIXTURES")

    def test_bundle_get(self):
        self._test_bundle_get("aws")
        self._test_bundle_get("gcp")

    def _test_bundle_get(self, replica):
        bundle_uuid = "011c7340-9b3c-4d62-bf49-090d79daf198"
        version = "2017-06-20T214506.766634Z"

        url = str(UrlBuilder()
                  .set(path="/v1/bundles/" + bundle_uuid)
                  .add_query("replica", replica)
                  .add_query("version", version))

        with override_bucket_config(DeploymentStage.TEST_FIXTURE):
            resp_obj = self.assertGetResponse(
                url,
                requests.codes.ok)

        self.assertEqual(resp_obj.json['bundle']['uuid'], bundle_uuid)
        self.assertEqual(resp_obj.json['bundle']['version'], version)
        self.assertEqual(resp_obj.json['bundle']['creator_uid'], 12345)
        self.assertEqual(resp_obj.json['bundle']['files'][0]['content-type'], "text/plain")
        self.assertEqual(resp_obj.json['bundle']['files'][0]['crc32c'], "e16e07b9")
        self.assertEqual(resp_obj.json['bundle']['files'][0]['name'], "LICENSE")
        self.assertEqual(resp_obj.json['bundle']['files'][0]['s3_etag'], "3b83ef96387f14655fc854ddc3c6bd57")
        self.assertEqual(resp_obj.json['bundle']['files'][0]['sha1'], "2b8b815229aa8a61e483fb4ba0588b8b6c491890")
        self.assertEqual(resp_obj.json['bundle']['files'][0]['sha256'],
                         "cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30")
        self.assertEqual(resp_obj.json['bundle']['files'][0]['uuid'], "ce55fd51-7833-469b-be0b-5da88ebebfcd")
        self.assertEqual(resp_obj.json['bundle']['files'][0]['version'], "2017-06-16T193604.240704Z")

    def test_bundle_get_directaccess(self):
        self._test_bundle_get_directaccess("aws")
        self._test_bundle_get_directaccess("gcp")

    def _test_bundle_get_directaccess(self, replica):
        if replica == "aws":
            schema = "s3"
        elif replica == "gcp":
            schema = "gs"

        bundle_uuid = "011c7340-9b3c-4d62-bf49-090d79daf198"
        version = "2017-06-20T214506.766634Z"

        url = str(UrlBuilder()
                  .set(path="/v1/bundles/" + bundle_uuid)
                  .add_query("replica", replica)
                  .add_query("version", version)
                  .add_query("directurls", "true"))

        with override_bucket_config(DeploymentStage.TEST_FIXTURE):
            resp_obj = self.assertGetResponse(
                url,
                requests.codes.ok)

        url = resp_obj.json['bundle']['files'][0]['url']
        splitted = urllib.parse.urlparse(url)
        self.assertEqual(splitted.scheme, schema)
        bucket = splitted.netloc
        key = splitted.path[1:]  # ignore the / part of the path.

        handle, _, _ = Config.get_cloud_specific_handles(replica)
        contents = handle.get(bucket, key)

        hasher = hashlib.sha1()
        hasher.update(contents)
        sha1 = hasher.hexdigest()
        self.assertEqual(sha1, "2b8b815229aa8a61e483fb4ba0588b8b6c491890")

    def test_bundle_put(self):
        self._test_bundle_put("aws", self.s3_test_fixtures_bucket)
        self._test_bundle_put("gcp", self.gs_test_fixtures_bucket)

    def _test_bundle_put(self, replica, fixtures_bucket):
        if replica == "aws":
            schema = "s3"
        elif replica == "gcp":
            schema = "gs"

        def upload_bundle(
                bundle_uuid: str,
                files: typing.Iterable[typing.Tuple[str, str, str]],
                bundle_version: typing.Optional[str]=None,
                expected_code: int=requests.codes.created):
            builder = (UrlBuilder()
                       .set(path="/v1/bundles/" + bundle_uuid)
                       .add_query("replica", replica))
            if bundle_version is not None:
                builder.add_query("version", bundle_version)
            url = str(builder)

            resp_obj = self.assertPutResponse(
                url,
                expected_code,
                json_request_body=dict(
                    files=[
                        dict(
                            uuid=file_uuid,
                            version=file_version,
                            name=file_name,
                            indexed=False,
                        )
                        for file_uuid, file_version, file_name in files
                    ],
                    creator_uid=12345,
                ),
            )

            if 200 <= resp_obj.response.status_code < 300:
                self.assertHeaders(
                    resp_obj.response,
                    {
                        'content-type': "application/json",
                    }
                )
                self.assertIn('version', resp_obj.json)

            return resp_obj

        bundle_uuid = str(uuid.uuid4())
        file_uuid = str(uuid.uuid4())
        resp_obj = self.upload_file_wait(
            f"{schema}://{fixtures_bucket}/test_good_source_data/0",
            replica,
            file_uuid,
            bundle_uuid,
        )
        file_version = resp_obj.json['version']

        # first bundle.
        bundle_version = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H%M%S.%fZ")
        upload_bundle(
            bundle_uuid,
            [(file_uuid, file_version, "LICENSE")],
            bundle_version,
        )

        # should be able to do this twice (i.e., same payload, same UUIDs)
        upload_bundle(
            bundle_uuid,
            [(file_uuid, file_version, "LICENSE")],
            bundle_version,
            requests.codes.ok,
        )

        # should *NOT* be able to do this twice with different payload.
        upload_bundle(
            bundle_uuid,
            [(file_uuid, file_version, "LICENSE1")],
            bundle_version,
            requests.codes.conflict,
        )

    def test_no_replica(self):
        """
        Verify we raise the correct error code when we provide no replica.
        """
        bundle_uuid = "ce55fd51-7833-469b-be0b-5da88ec0ffee"

        url = str(UrlBuilder()
                  .set(path="/v1/bundles/" + bundle_uuid))

        with override_bucket_config(DeploymentStage.TEST_FIXTURE):
            self.assertPutResponse(
                url,
                requests.codes.bad_request,
                json_request_body=dict(
                    files=[],
                    creator_uid=12345,
                ),
                expected_error=ExpectedErrorFields(
                    code="illegal_arguments",
                    status=requests.codes.bad_request,
                    expect_stacktrace=True)
            )

    def test_no_files(self):
        """
        Verify we raise the correct error code when we do not provide the list of files.
        """
        bundle_uuid = "ce55fd51-7833-469b-be0b-5da88ec0ffee"

        url = str(UrlBuilder()
                  .set(path="/v1/bundles/" + bundle_uuid)
                  .add_query("replica", "aws"))

        with override_bucket_config(DeploymentStage.TEST_FIXTURE):
            self.assertPutResponse(
                url,
                requests.codes.bad_request,
                json_request_body=dict(
                    creator_uid=12345,
                ),
                expected_error=ExpectedErrorFields(
                    code="illegal_arguments",
                    status=requests.codes.bad_request,
                    expect_stacktrace=True)
            )

    def test_bundle_get_not_found(self):
        """
        Verify that we return the correct error message when the bundle cannot be found.
        """
        self._test_bundle_get_not_found("aws")
        self._test_bundle_get_not_found("gcp")

    def _test_bundle_get_not_found(self, replica):
        bundle_uuid = str(uuid.uuid4())

        url = str(UrlBuilder()
                  .set(path="/v1/bundles/" + bundle_uuid)
                  .add_query("replica", replica))

        with override_bucket_config(DeploymentStage.TEST_FIXTURE):
            self.assertGetResponse(
                url,
                requests.codes.not_found,
                expected_error=ExpectedErrorFields(
                    code="not_found",
                    status=requests.codes.not_found)
            )

        version = "2017-06-16T193604.240704Z"
        url = str(UrlBuilder()
                  .set(path="/v1/bundles/" + bundle_uuid)
                  .add_query("replica", replica)
                  .add_query("version", version))

        with override_bucket_config(DeploymentStage.TEST_FIXTURE):
            self.assertGetResponse(
                url,
                requests.codes.not_found,
                expected_error=ExpectedErrorFields(
                    code="not_found",
                    status=requests.codes.not_found)
            )


if __name__ == '__main__':
    unittest.main()
