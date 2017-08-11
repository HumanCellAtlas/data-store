#!/usr/bin/env python
# coding: utf-8

import hashlib
import os
import sys
import tempfile
import unittest
import uuid

import requests

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss.config import DeploymentStage, override_bucket_config
from dss.util import UrlBuilder
from tests.fixtures.cloud_uploader import GSUploader, S3Uploader, Uploader
from tests.infra import DSSAsserts, ExpectedErrorFields, get_env, generate_test_key


class TestFileApi(unittest.TestCase, DSSAsserts):
    def setUp(self):
        self.app = dss.create_app().app.test_client()
        dss.Config.set_config(dss.DeploymentStage.TEST)
        self.s3_test_fixtures_bucket = get_env("DSS_S3_BUCKET_TEST_FIXTURES")
        self.gs_test_fixtures_bucket = get_env("DSS_GS_BUCKET_TEST_FIXTURES")
        self.s3_test_bucket = get_env("DSS_S3_BUCKET_TEST")
        self.gs_test_bucket = get_env("DSS_GS_BUCKET_TEST")

    def test_file_put(self):
        tempdir = tempfile.gettempdir()
        self._test_file_put("s3", self.s3_test_bucket, S3Uploader(tempdir, self.s3_test_bucket))
        self._test_file_put("gs", self.gs_test_bucket, GSUploader(tempdir, self.gs_test_bucket))

    def _test_file_put(self, scheme: str, test_bucket: str, uploader: Uploader):
        src_key = generate_test_key()
        src_data = os.urandom(1024)
        with tempfile.NamedTemporaryFile(delete=True) as fh:
            fh.write(src_data)
            fh.flush()

            uploader.checksum_and_upload_file(
                fh.name, src_key, {"hca-dss-content-type": "text/plain", })

        # should be able to do this twice (i.e., same payload, different UUIDs)
        for _ in range(2):
            resp_obj = self.assertPutResponse(
                "/v1/files/" + str(uuid.uuid4()),
                requests.codes.created,
                json_request_body=dict(
                    source_url=f"{scheme}://{test_bucket}/{src_key}",
                    bundle_uuid=str(uuid.uuid4()),
                    creator_uid=4321,
                    content_type="text/html",
                ),
            )
            self.assertHeaders(
                resp_obj.response,
                {
                    'content-type': "application/json",
                }
            )
            self.assertIn('version', resp_obj.json)

    # This is a test specific to AWS since it has separate notion of metadata and tags.
    def test_file_put_metadata_from_tags(self):
        file_uuid = uuid.uuid4()
        resp_obj = self.assertPutResponse(
            "/v1/files/" + str(file_uuid),
            requests.codes.created,
            json_request_body=dict(
                source_url=f"s3://{self.s3_test_fixtures_bucket}/test_good_source_data/metadata_in_tags",
                bundle_uuid=str(uuid.uuid4()),
                creator_uid=4321,
                content_type="text/html",
            ),
        )
        self.assertHeaders(
            resp_obj.response,
            {
                'content-type': "application/json",
            }
        )
        self.assertIn('version', resp_obj.json)

    def test_file_put_upper_case_checksums(self):
        self._test_file_put_upper_case_checksums("s3", self.s3_test_fixtures_bucket)
        self._test_file_put_upper_case_checksums("gs", self.gs_test_fixtures_bucket)

    def _test_file_put_upper_case_checksums(self, scheme, fixtures_bucket):
        file_uuid = uuid.uuid4()
        resp_obj = self.assertPutResponse(
            "/v1/files/" + str(file_uuid),
            requests.codes.created,
            json_request_body=dict(
                source_url=f"{scheme}://{fixtures_bucket}/test_good_source_data/incorrect_case_checksum",
                bundle_uuid=str(uuid.uuid4()),
                creator_uid=4321,
                content_type="text/html",
            ),
        )
        self.assertHeaders(
            resp_obj.response,
            {
                'content-type': "application/json",
            }
        )
        self.assertIn('version', resp_obj.json)

    def test_file_head(self):
        self._test_file_head("aws")
        self._test_file_head("gcp")

    def _test_file_head(self, replica):
        file_uuid = "ce55fd51-7833-469b-be0b-5da88ebebfcd"
        version = "2017-06-16T193604.240704Z"

        url = str(UrlBuilder()
                  .set(path="/v1/files/" + file_uuid)
                  .add_query("replica", replica)
                  .add_query("version", version))

        with override_bucket_config(DeploymentStage.TEST_FIXTURE):
            self.assertHeadResponse(
                url,
                requests.codes.ok
            )

            # TODO: (ttung) verify headers

    def test_file_get_specific(self):
        self._test_file_get_specific("aws")
        self._test_file_get_specific("gcp")

    def _test_file_get_specific(self, replica):
        """
        Verify we can successfully fetch a specific file UUID+version.
        """
        file_uuid = "ce55fd51-7833-469b-be0b-5da88ebebfcd"
        version = "2017-06-16T193604.240704Z"

        url = str(UrlBuilder()
                  .set(path="/v1/files/" + file_uuid)
                  .add_query("replica", replica)
                  .add_query("version", version))

        with override_bucket_config(DeploymentStage.TEST_FIXTURE):
            resp_obj = self.assertGetResponse(
                url,
                requests.codes.found
            )

            url = resp_obj.response.headers['Location']
            sha1 = resp_obj.response.headers['X-DSS-SHA1']
            data = requests.get(url)
            self.assertEqual(len(data.content), 11358)

            # verify that the downloaded data matches the stated checksum
            hasher = hashlib.sha1()
            hasher.update(data.content)
            self.assertEqual(hasher.hexdigest(), sha1)

            # TODO: (ttung) verify more of the headers

    def test_file_get_latest(self):
        self._test_file_get_latest("aws")
        self._test_file_get_latest("gcp")

    def _test_file_get_latest(self, replica):
        """
        Verify we can successfully fetch the latest version of a file UUID.
        """
        file_uuid = "ce55fd51-7833-469b-be0b-5da88ebebfcd"

        url = str(UrlBuilder()
                  .set(path="/v1/files/" + file_uuid)
                  .add_query("replica", replica))

        with override_bucket_config(DeploymentStage.TEST_FIXTURE):
            resp_obj = self.assertGetResponse(
                url,
                requests.codes.found
            )

            url = resp_obj.response.headers['Location']
            sha1 = resp_obj.response.headers['X-DSS-SHA1']
            data = requests.get(url)
            self.assertEqual(len(data.content), 8685)

            # verify that the downloaded data matches the stated checksum
            hasher = hashlib.sha1()
            hasher.update(data.content)
            self.assertEqual(hasher.hexdigest(), sha1)

            # TODO: (ttung) verify more of the headers

    def test_file_get_not_found(self):
        self._test_file_get_not_found("aws")
        self._test_file_get_not_found("gcp")

    def _test_file_get_not_found(self, replica):
        """
        Verify we can successfully fetch the latest version of a file UUID.
        """
        file_uuid = "ce55fd51-7833-469b-be0b-5da88ec0ffee"

        url = str(UrlBuilder()
                  .set(path="/v1/files/" + file_uuid)
                  .add_query("replica", replica))

        with override_bucket_config(DeploymentStage.TEST_FIXTURE):
            self.assertGetResponse(
                url,
                requests.codes.not_found,
                expected_error=ExpectedErrorFields(
                    code="not_found",
                    status=requests.codes.not_found,
                    expect_stacktrace=True)
            )

    def test_file_get_no_replica(self):
        """
        Verify we raise the correct error code when we provide no replica.
        """
        file_uuid = "ce55fd51-7833-469b-be0b-5da88ec0ffee"

        url = str(UrlBuilder()
                  .set(path="/v1/files/" + file_uuid))

        with override_bucket_config(DeploymentStage.TEST_FIXTURE):
            self.assertGetResponse(
                url,
                requests.codes.bad_request,
                expected_error=ExpectedErrorFields(
                    code="illegal_arguments",
                    status=requests.codes.bad_request,
                    expect_stacktrace=True)
            )

if __name__ == '__main__':
    unittest.main()
