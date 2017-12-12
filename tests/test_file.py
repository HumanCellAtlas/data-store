#!/usr/bin/env python
# coding: utf-8

import typing
import datetime
import hashlib
import os
import sys
import tempfile
import unittest
import uuid

import requests
from dss.util.version import datetime_to_version_format

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss.config import BucketConfig, override_bucket_config
from dss.util import UrlBuilder
from dss.util.aws import AWS_MIN_CHUNK_SIZE
from tests.fixtures.cloud_uploader import GSUploader, S3Uploader, Uploader
from tests.infra import DSSAssertMixin, DSSUploadMixin, ExpectedErrorFields, get_env, generate_test_key, testmode
from tests.infra.server import ThreadedLocalServer


class TestFileApi(unittest.TestCase, DSSAssertMixin, DSSUploadMixin):
    @classmethod
    def setUpClass(cls):
        cls.app = ThreadedLocalServer()
        cls.app.start()

    @classmethod
    def tearDownClass(cls):
        cls.app.shutdown()

    def setUp(self):
        dss.Config.set_config(dss.BucketConfig.TEST)
        self.s3_test_fixtures_bucket = get_env("DSS_S3_BUCKET_TEST_FIXTURES")
        self.gs_test_fixtures_bucket = get_env("DSS_GS_BUCKET_TEST_FIXTURES")
        self.s3_test_bucket = get_env("DSS_S3_BUCKET_TEST")
        self.gs_test_bucket = get_env("DSS_GS_BUCKET_TEST")

    @testmode.standalone
    def test_file_put(self):
        tempdir = tempfile.gettempdir()
        self._test_file_put("aws", "s3", self.s3_test_bucket, S3Uploader(tempdir, self.s3_test_bucket))
        self._test_file_put("gcp", "gs", self.gs_test_bucket, GSUploader(tempdir, self.gs_test_bucket))

    def _test_file_put(self, replica: str, scheme: str, test_bucket: str, uploader: Uploader):
        src_key = generate_test_key()
        src_data = os.urandom(1024)
        with tempfile.NamedTemporaryFile(delete=True) as fh:
            fh.write(src_data)
            fh.flush()

            uploader.checksum_and_upload_file(fh.name, src_key, "text/plain")

        source_url = f"{scheme}://{test_bucket}/{src_key}"

        file_uuid = str(uuid.uuid4())
        bundle_uuid = str(uuid.uuid4())
        version = datetime_to_version_format(datetime.datetime.utcnow())

        # should be able to do this twice (i.e., same payload, different UUIDs)
        self.upload_file(source_url, file_uuid, bundle_uuid=bundle_uuid, version=version)
        self.upload_file(source_url, str(uuid.uuid4()))

        # should be able to do this twice (i.e., same payload, same UUIDs)
        self.upload_file(source_url, file_uuid, bundle_uuid=bundle_uuid,
                         version=version, expected_code=requests.codes.ok)

        # should *NOT* be able to do this twice (i.e., different payload, same UUIDs)
        self.upload_file(source_url, file_uuid, version=version, expected_code=requests.codes.conflict)

    @testmode.integration
    def test_file_put_large(self):
        tempdir = tempfile.gettempdir()
        self._test_file_put_large("aws", "s3", self.s3_test_bucket, S3Uploader(tempdir, self.s3_test_bucket))
        # There's no equivalent for GCP ... yet. :)

    def _test_file_put_large(self, replica: str, scheme: str, test_bucket: str, uploader: Uploader):
        src_key = generate_test_key()
        src_data = os.urandom(AWS_MIN_CHUNK_SIZE + 1)
        with tempfile.NamedTemporaryFile(delete=True) as fh:
            fh.write(src_data)
            fh.flush()

            uploader.checksum_and_upload_file(fh.name, src_key, "text/plain")

        # should be able to do this twice (i.e., same payload, different UUIDs).  first time should be asynchronous
        # since it's new data.  second time should be synchronous since the data is present.
        for expect_async in (True, False):
            resp_obj = self.upload_file_wait(f"{scheme}://{test_bucket}/{src_key}", replica, expect_async=expect_async)
            self.assertHeaders(
                resp_obj.response,
                {
                    'content-type': "application/json",
                }
            )
            self.assertIn('version', resp_obj.json)

    # This is a test specific to AWS since it has separate notion of metadata and tags.
    @testmode.standalone
    def test_file_put_metadata_from_tags(self):
        resp_obj = self.upload_file_wait(
            f"s3://{self.s3_test_fixtures_bucket}/test_good_source_data/metadata_in_tags",
            "aws",
        )
        self.assertHeaders(
            resp_obj.response,
            {
                'content-type': "application/json",
            }
        )
        self.assertIn('version', resp_obj.json)

    @testmode.standalone
    def test_file_put_upper_case_checksums(self):
        self._test_file_put_upper_case_checksums("s3", self.s3_test_fixtures_bucket)
        self._test_file_put_upper_case_checksums("gs", self.gs_test_fixtures_bucket)

    def _test_file_put_upper_case_checksums(self, scheme, fixtures_bucket):
        resp_obj = self.upload_file_wait(
            f"{scheme}://{fixtures_bucket}/test_good_source_data/incorrect_case_checksum",
            "aws",
        )
        self.assertHeaders(
            resp_obj.response,
            {
                'content-type': "application/json",
            }
        )
        self.assertIn('version', resp_obj.json)

    @testmode.standalone
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

        with override_bucket_config(BucketConfig.TEST_FIXTURE):
            self.assertHeadResponse(
                url,
                requests.codes.ok
            )

            # TODO: (ttung) verify headers

    @testmode.standalone
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

        with override_bucket_config(BucketConfig.TEST_FIXTURE):
            resp_obj = self.assertGetResponse(
                url,
                requests.codes.found
            )

            url = resp_obj.response.headers['Location']
            sha1 = resp_obj.response.headers['X-DSS-SHA1']
            data = requests.get(url)
            self.assertEqual(len(data.content), 11358)
            self.assertEqual(resp_obj.response.headers['X-DSS-SIZE'], '11358')

            # verify that the downloaded data matches the stated checksum
            hasher = hashlib.sha1()
            hasher.update(data.content)
            self.assertEqual(hasher.hexdigest(), sha1)

            # TODO: (ttung) verify more of the headers

    @testmode.standalone
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

        with override_bucket_config(BucketConfig.TEST_FIXTURE):
            resp_obj = self.assertGetResponse(
                url,
                requests.codes.found
            )

            url = resp_obj.response.headers['Location']
            sha1 = resp_obj.response.headers['X-DSS-SHA1']
            data = requests.get(url)
            self.assertEqual(len(data.content), 8685)
            self.assertEqual(resp_obj.response.headers['X-DSS-SIZE'], '8685')

            # verify that the downloaded data matches the stated checksum
            hasher = hashlib.sha1()
            hasher.update(data.content)
            self.assertEqual(hasher.hexdigest(), sha1)

            # TODO: (ttung) verify more of the headers

    @testmode.standalone
    def test_file_get_not_found(self):
        """
        Verify that we return the correct error message when the file cannot be found.
        """
        self._test_file_get_not_found("aws")
        self._test_file_get_not_found("gcp")

    def _test_file_get_not_found(self, replica):
        file_uuid = "ce55fd51-7833-469b-be0b-5da88ec0ffee"

        url = str(UrlBuilder()
                  .set(path="/v1/files/" + file_uuid)
                  .add_query("replica", replica))

        with override_bucket_config(BucketConfig.TEST_FIXTURE):
            self.assertGetResponse(
                url,
                requests.codes.not_found,
                expected_error=ExpectedErrorFields(
                    code="not_found",
                    status=requests.codes.not_found,
                    expect_stacktrace=True)
            )

        version = "2017-06-16T193604.240704Z"
        url = str(UrlBuilder()
                  .set(path="/v1/files/" + file_uuid)
                  .add_query("replica", replica)
                  .add_query("version", version))

        with override_bucket_config(BucketConfig.TEST_FIXTURE):
            self.assertGetResponse(
                url,
                requests.codes.not_found,
                expected_error=ExpectedErrorFields(
                    code="not_found",
                    status=requests.codes.not_found,
                    expect_stacktrace=True)
            )

    @testmode.standalone
    def test_file_get_no_replica(self):
        """
        Verify we raise the correct error code when we provide no replica.
        """
        file_uuid = "ce55fd51-7833-469b-be0b-5da88ec0ffee"

        url = str(UrlBuilder()
                  .set(path="/v1/files/" + file_uuid))

        with override_bucket_config(BucketConfig.TEST_FIXTURE):
            self.assertGetResponse(
                url,
                requests.codes.bad_request,
                expected_error=ExpectedErrorFields(
                    code="illegal_arguments",
                    status=requests.codes.bad_request,
                    expect_stacktrace=True)
            )

    @testmode.standalone
    def test_file_size(self):
        """
        Verify size is correct after dss put and get
        """
        tempdir = tempfile.gettempdir()
        self._test_file_size("aws", "s3", self.s3_test_bucket, S3Uploader(tempdir, self.s3_test_bucket))
        self._test_file_size("gcp", "gs", self.gs_test_bucket, GSUploader(tempdir, self.gs_test_bucket))

    def _test_file_size(self, replica: str, scheme: str, test_bucket: str, uploader: Uploader):
        src_key = generate_test_key()
        src_size = 1024 + int.from_bytes(os.urandom(1), byteorder='little')
        src_data = os.urandom(src_size)
        with tempfile.NamedTemporaryFile(delete=True) as fh:
            fh.write(src_data)
            fh.flush()

            uploader.checksum_and_upload_file(fh.name, src_key, "text/plain")

        source_url = f"{scheme}://{test_bucket}/{src_key}"

        file_uuid = str(uuid.uuid4())
        bundle_uuid = str(uuid.uuid4())
        version = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H%M%S.%fZ")

        self.upload_file(source_url, file_uuid, bundle_uuid=bundle_uuid, version=version)

        url = str(UrlBuilder()
                  .set(path="/v1/files/" + file_uuid)
                  .add_query("replica", replica))

        with override_bucket_config(BucketConfig.TEST):
            resp_obj = self.assertGetResponse(
                url,
                requests.codes.found
            )

            url = resp_obj.response.headers['Location']
            data = requests.get(url)
            self.assertEqual(len(data.content), src_size)
            self.assertEqual(resp_obj.response.headers['X-DSS-SIZE'], str(src_size))

    def upload_file(
            self: typing.Any,
            source_url: str,
            file_uuid: str,
            bundle_uuid: str=None,
            version: str=None,
            expected_code: int=requests.codes.created,
    ):
        bundle_uuid = str(uuid.uuid4()) if bundle_uuid is None else bundle_uuid
        if version is None:
            timestamp = datetime.datetime.utcnow()
            version = timestamp.strftime("%Y-%m-%dT%H%M%S.%fZ")

        urlbuilder = UrlBuilder().set(path='/v1/files/' + file_uuid)
        urlbuilder.add_query("version", version)

        resp_obj = self.assertPutResponse(
            str(urlbuilder),
            expected_code,
            json_request_body=dict(
                bundle_uuid=bundle_uuid,
                creator_uid=0,
                source_url=source_url,
            ),
        )

        if resp_obj.response.status_code == requests.codes.created:
            self.assertHeaders(
                resp_obj.response,
                {
                    'content-type': "application/json",
                }
            )
            self.assertIn('version', resp_obj.json)

if __name__ == '__main__':
    unittest.main()
