#!/usr/bin/env python
# coding: utf-8

import datetime
import hashlib
import json
import os
import requests
import sys
import tempfile
import typing
import unittest
from unittest import mock
import uuid

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss.api.files import ASYNC_COPY_THRESHOLD, RETRY_AFTER_INTERVAL
from dss.config import BucketConfig, Config, override_bucket_config, Replica
from dss.storage.hcablobstore import compose_blob_key
from dss.util import UrlBuilder
from dss.util.version import datetime_to_version_format
from tests import eventually, get_auth_header
from tests.fixtures.cloud_uploader import GSUploader, S3Uploader, Uploader
from tests.infra import DSSAssertMixin, DSSUploadMixin, ExpectedErrorFields, get_env, generate_test_key, testmode, \
    TestAuthMixin
from tests.infra.server import ThreadedLocalServer


# Max number of retries
FILE_GET_RETRY_COUNT = 10


class TestFileApi(unittest.TestCase, TestAuthMixin, DSSUploadMixin, DSSAssertMixin):
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
        self._test_file_put(Replica.aws, "s3", self.s3_test_bucket, S3Uploader(tempdir, self.s3_test_bucket))
        self._test_file_put(Replica.gcp, "gs", self.gs_test_bucket, GSUploader(tempdir, self.gs_test_bucket))

    def _test_put_auth_errors(self, scheme, test_bucket):
        src_key = generate_test_key()
        source_url = f"{scheme}://{test_bucket}/{src_key}"

        file_uuid = str(uuid.uuid4())
        bundle_uuid = str(uuid.uuid4())
        timestamp = datetime.datetime.utcnow()
        version = timestamp.strftime("%Y-%m-%dT%H%M%S.%fZ")

        urlbuilder = UrlBuilder().set(path='/v1/files/' + file_uuid)
        urlbuilder.add_query("version", version)
        self._test_auth_errors('put',
                               str(urlbuilder),
                               json_request_body=dict(
                                   bundle_uuid=bundle_uuid,
                                   creator_uid=0,
                                   source_url=source_url)
                               )

    def _test_file_put(self, replica: Replica, scheme: str, test_bucket: str, uploader: Uploader):
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

        self._test_put_auth_errors(scheme, test_bucket)

        with self.subTest(f"{replica}: Created returned when uploading a file with a unique payload, and FQID"):
            self.upload_file(source_url, file_uuid, bundle_uuid=bundle_uuid, version=version)

        with self.subTest(f"{replica}: Created returned when uploading a file with same payload, and different FQID"):
            self.upload_file(source_url, str(uuid.uuid4()))

        with self.subTest(f"{replica}: OK returned when uploading a file with the same payload, UUID,  version"):
            self.upload_file(source_url, file_uuid, bundle_uuid=bundle_uuid,
                             version=version, expected_code=requests.codes.ok)

        with self.subTest(f"{replica}: Conflict returned when uploading a file with a different payload and same FQID"):
            src_key_temp = generate_test_key()
            src_data_temp = os.urandom(128)
            with tempfile.NamedTemporaryFile(delete=True) as fh:
                fh.write(src_data_temp)
                fh.flush()

                uploader.checksum_and_upload_file(fh.name, src_key_temp, "text/plain")

            source_url_temp = f"{scheme}://{test_bucket}/{src_key_temp}"
            self.upload_file(source_url_temp, file_uuid, version=version, expected_code=requests.codes.conflict)

        with self.subTest(f"{replica}: Bad returned when uploading a file with an invalid version"):
            self.upload_file(source_url, file_uuid, version='', expected_code=requests.codes.bad_request)

        invalid_version = 'ABCD'
        with self.subTest(f"{replica}: bad_request returned "
                          f"when uploading a file with invalid version {invalid_version}"):
            self.upload_file(source_url, file_uuid, version=invalid_version, expected_code=requests.codes.bad_request)

        with self.subTest(f"{replica}: Bad returned when uploading a file without a version"):
            self.upload_file(source_url, file_uuid, version='missing', expected_code=requests.codes.bad_request)

        invalid_uuids = ['ABCD', '1234']
        for invalid_uuid in invalid_uuids:
            with self.subTest(f"{replica}: Bad returned "
                              f"when uploading a file with invalid UUID {invalid_uuid}"):
                self.upload_file(source_url, invalid_uuid, expected_code=requests.codes.bad_request)

        with self.subTest(f"{replica}: forbidden returned "
                          f"when uploading a file with without UUID {invalid_uuid}"):
            self.upload_file(source_url, '', expected_code=requests.codes.forbidden)

    @staticmethod
    def _upload_file_to_mock_ingest(
            uploader_class: typing.Type[Uploader],
            bucket: str,
            key: str,
            data: bytes) -> None:
        tempdir = tempfile.gettempdir()
        uploader = uploader_class(tempdir, bucket)
        with tempfile.NamedTemporaryFile(delete=True) as fh:
            fh.write(data)
            fh.flush()

            uploader.checksum_and_upload_file(fh.name, key, "text/plain")

    @testmode.standalone
    def test_file_put_large_sync(self):
        """Test PUT /files with the largest file that is copied synchronously."""
        test_data = os.urandom(ASYNC_COPY_THRESHOLD)
        self._test_file_put_large(test_data[:-1])
        self._test_file_put_large(test_data)

    @testmode.integration
    def test_file_put_large_async(self):
        """Test PUT /files with the smallest file that is copied asynchronously."""
        test_data = os.urandom(ASYNC_COPY_THRESHOLD + 1)
        self._test_file_put_large(test_data)

    def _test_file_put_large(self, src_data: bytes) -> None:
        replicas: typing.Sequence[typing.Tuple[Replica, typing.Type[Uploader], str]] = [
            (Replica.aws, S3Uploader, self.s3_test_bucket),
            (Replica.gcp, GSUploader, self.gs_test_bucket)
        ]
        src_key = generate_test_key()
        for replica, uploader_class, bucket in replicas:
            self._upload_file_to_mock_ingest(uploader_class, bucket, src_key, src_data)

            expect_async_results: typing.Tuple[typing.Optional[bool], ...]
            if len(src_data) > ASYNC_COPY_THRESHOLD:
                if Replica == Replica.aws:
                    # We should be able to do this twice (i.e., same payload, different UUIDs).
                    # First time should be asynchronous since it's new data.  Second time should be
                    # synchronous since the data is present, but because S3 does not make
                    # consistency guarantees, a second client might not see that the data is already
                    # there.  Therefore, we do not mandate that it is done synchronously.
                    expect_async_results = (True, None)
                else:
                    # We should be able to do this twice (i.e., same payload, different UUIDs).
                    # First time should be asynchronous since it's new data.  Second time should be
                    # synchronous since the data is present.
                    expect_async_results = (True, False)
            else:
                # We should be able to do this twice (i.e., same payload, different UUIDs).  Neither
                # time should be asynchronous.
                expect_async_results = (False, False)

            for ix, expect_async in enumerate(expect_async_results):
                with self.subTest(f"replica: {replica.name} size: {len(src_data)} round: {ix}"):
                    resp_obj = self.upload_file_wait(
                        f"{replica.storage_schema}://{bucket}/{src_key}",
                        replica,
                        expect_async=expect_async)
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
            Replica.aws,
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
            Replica.aws,
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
        self._test_file_head(Replica.aws)
        self._test_file_head(Replica.gcp)

    def _test_file_head(self, replica: Replica):
        file_uuid = "ce55fd51-7833-469b-be0b-5da88ebebfcd"
        version = "2017-06-16T193604.240704Z"

        url = str(UrlBuilder()
                  .set(path="/v1/files/" + file_uuid)
                  .add_query("replica", replica.name)
                  .add_query("version", version))

        with override_bucket_config(BucketConfig.TEST_FIXTURE):
            self.assertHeadResponse(
                url,
                [requests.codes.ok, requests.codes.moved]
            )

            # TODO: (ttung) verify headers

    @testmode.standalone
    def test_file_get_specific(self):
        self._test_file_get_specific(Replica.aws)
        self._test_file_get_specific(Replica.gcp)

    def _test_file_get_specific(self, replica: Replica):
        """
        Verify we can successfully fetch a specific file UUID+version.
        """
        file_uuid = "ce55fd51-7833-469b-be0b-5da88ebebfcd"
        version = "2017-06-16T193604.240704Z"

        url = str(UrlBuilder()
                  .set(path="/v1/files/" + file_uuid)
                  .add_query("replica", replica.name)
                  .add_query("version", version))

        with override_bucket_config(BucketConfig.TEST_FIXTURE):
            resp_obj = self.assertGetResponse(
                url,
                requests.codes.found,
                redirect_follow_retries=FILE_GET_RETRY_COUNT,
                min_retry_interval_header=RETRY_AFTER_INTERVAL,
                override_retry_interval=1,
            )
            if resp_obj.response.status_code == requests.codes.found:
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
                return
        self.fail(f"Failed after {FILE_GET_RETRY_COUNT} retries.")

    @testmode.standalone
    def test_file_get_latest(self):
        self._test_file_get_latest(Replica.aws)
        self._test_file_get_latest(Replica.gcp)

    def _test_file_get_latest(self, replica: Replica):
        """
        Verify we can successfully fetch the latest version of a file UUID.
        """
        file_uuid = "ce55fd51-7833-469b-be0b-5da88ebebfcd"

        url = str(UrlBuilder()
                  .set(path="/v1/files/" + file_uuid)
                  .add_query("replica", replica.name))

        with override_bucket_config(BucketConfig.TEST_FIXTURE):
            resp_obj = self.assertGetResponse(
                url,
                requests.codes.found,
                redirect_follow_retries=FILE_GET_RETRY_COUNT,
                min_retry_interval_header=RETRY_AFTER_INTERVAL,
                override_retry_interval=1,
            )
            if resp_obj.response.status_code == requests.codes.found:
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
                return
        self.fail(f"Failed after {FILE_GET_RETRY_COUNT} retries.")

    @testmode.standalone
    def test_file_get_not_found(self):
        """
        Verify that we return the correct error message when the file cannot be found.
        """
        self._test_file_get_not_found(Replica.aws)
        self._test_file_get_not_found(Replica.gcp)

    def _test_file_get_not_found(self, replica: Replica):
        file_uuid = "ce55fd51-7833-469b-be0b-5da88ec0ffee"

        url = str(UrlBuilder()
                  .set(path="/v1/files/" + file_uuid)
                  .add_query("replica", replica.name))

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
                  .add_query("replica", replica.name)
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
    def test_file_get_invalid_token(self):
        """
        Verifies that a checkout request with a malformed token returns a 400.
        :return:
        """
        tempdir = tempfile.gettempdir()
        self._test_file_get_invalid_token(Replica.aws,
                                          "s3",
                                          self.s3_test_bucket,
                                          S3Uploader(tempdir, self.s3_test_bucket))
        self._test_file_get_invalid_token(Replica.gcp,
                                          "gs",
                                          self.gs_test_bucket,
                                          GSUploader(tempdir, self.gs_test_bucket))

    def _test_file_get_invalid_token(self, replica: Replica, scheme: str, test_bucket: str, uploader: Uploader):
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
        url = str(UrlBuilder()
                  .set(path="/v1/files/" + file_uuid)
                  .add_query("replica", replica.name)
                  .add_query("version", version)
                  .add_query("token", "{}"))

        @eventually(30, 0.1)
        def try_get():
            self.assertGetResponse(
                url, requests.codes.bad_request)
        try_get()

    @testmode.integration
    def test_file_get_checkout(self):
        """
        Verifies checkout occurs on first get and not on second.
        """
        tempdir = tempfile.gettempdir()
        self._test_file_get_checkout(Replica.aws, "s3", self.s3_test_bucket,
                                     S3Uploader(tempdir, self.s3_test_bucket))
        self._test_file_get_checkout(Replica.gcp, "gs", self.gs_test_bucket,
                                     GSUploader(tempdir, self.gs_test_bucket))

    def _test_file_get_checkout(self, replica: Replica, scheme: str, test_bucket: str, uploader: Uploader):
        handle = Config.get_blobstore_handle(replica)
        src_key = generate_test_key()
        src_data = os.urandom(1024)
        source_url = f"{scheme}://{test_bucket}/{src_key}"
        file_uuid = str(uuid.uuid4())
        bundle_uuid = str(uuid.uuid4())
        version = datetime_to_version_format(datetime.datetime.utcnow())

        # write dummy file and upload to upload area
        with tempfile.NamedTemporaryFile(delete=True) as fh:
            fh.write(src_data)
            fh.flush()

            uploader.checksum_and_upload_file(fh.name, src_key, "text/plain")

        # upload file to DSS
        self.upload_file(source_url, file_uuid, bundle_uuid=bundle_uuid, version=version)
        url = str(UrlBuilder()
                  .set(path="/v1/files/" + file_uuid)
                  .add_query("replica", replica.name)
                  .add_query("version", version))

        # get uploaded blob key
        file_metadata = json.loads(
            handle.get(
                test_bucket,
                f"files/{file_uuid}.{version}"
            ).decode("utf-8"))
        file_key = compose_blob_key(file_metadata)

        @eventually(10, 1)
        def test_checkout():
            # assert 302 and verify checksum on checkout completion
            api_get = self.assertGetResponse(
                url, requests.codes.found, redirect_follow_retries=0)
            file_get = requests.get(api_get.response.headers['Location'])
            self.assertTrue(file_get.ok)
            self.assertEquals(file_get.content, src_data)

        with self.subTest(f"{replica}: Initiates checkout and returns 301 for GET on 'uncheckedout' file."):
            # assert 301 redirect on first GET
            self.assertGetResponse(url, requests.codes.moved, redirect_follow_retries=0)
            test_checkout()

        with self.subTest(f"{replica}: Initiates checkout and returns 301 for GET on nearly expired checkout file."):
            now = datetime.datetime.now(datetime.timezone.utc)
            creation_date_fn = ("cloud_blobstore.s3.S3BlobStore.get_creation_date"
                                if replica.name == "aws"
                                else "cloud_blobstore.gs.GSBlobStore.get_creation_date")
            with mock.patch(creation_date_fn) as mock_creation_date:
                blob_ttl_days = int(os.environ['DSS_BLOB_TTL_DAYS'])
                mock_creation_date.return_value = now - datetime.timedelta(days=blob_ttl_days, hours=1, minutes=5)
                self.assertGetResponse(url, requests.codes.moved, redirect_follow_retries=0)
            test_checkout()

        with self.subTest(f"{replica}: Initiates checkout and returns 302 immediately for GET on stale checkout file."):
            @eventually(10, 1)
            def test_creation_date_updated(key, prev_creation_date):
                self.assertTrue(prev_creation_date < handle.get_creation_date(replica.checkout_bucket, key))

            now = datetime.datetime.now(datetime.timezone.utc)
            old_creation_date = handle.get_creation_date(replica.checkout_bucket, file_key)
            creation_date_fn = ("cloud_blobstore.s3.S3BlobStore.get_creation_date"
                                if replica.name == "aws"
                                else "cloud_blobstore.gs.GSBlobStore.get_creation_date")
            with mock.patch(creation_date_fn) as mock_creation_date:
                # assert 302 found on stale file and that last modified refreshes
                blob_ttl_days = int(os.environ['DSS_BLOB_PUBLIC_TTL_DAYS'])
                mock_creation_date.return_value = now - datetime.timedelta(days=blob_ttl_days + 1)
                self.assertGetResponse(url, requests.codes.found, redirect_follow_retries=0)
            test_creation_date_updated(file_key, old_creation_date)

        handle.delete(test_bucket, f"files/{file_uuid}.{version}")
        handle.delete(replica.checkout_bucket, file_key)

    @testmode.standalone
    def test_file_size(self):
        """
        Verify size is correct after dss put and get
        """
        tempdir = tempfile.gettempdir()
        self._test_file_size(Replica.aws, "s3", self.s3_test_bucket, S3Uploader(tempdir, self.s3_test_bucket))
        self._test_file_size(Replica.gcp, "gs", self.gs_test_bucket, GSUploader(tempdir, self.gs_test_bucket))

    def _test_file_size(self, replica: Replica, scheme: str, test_bucket: str, uploader: Uploader):
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
                  .add_query("replica", replica.name))

        with override_bucket_config(BucketConfig.TEST):
            resp_obj = self.assertGetResponse(
                url,
                requests.codes.found,
                redirect_follow_retries=FILE_GET_RETRY_COUNT,
                min_retry_interval_header=RETRY_AFTER_INTERVAL,
                override_retry_interval=1,
            )
            if resp_obj.response.status_code == requests.codes.found:
                url = resp_obj.response.headers['Location']
                data = requests.get(url)
                self.assertEqual(len(data.content), src_size)
                self.assertEqual(resp_obj.response.headers['X-DSS-SIZE'], str(src_size))
                return
        self.fail(f"Failed after {FILE_GET_RETRY_COUNT} retries.")

    def upload_file(
            self: typing.Any,
            source_url: str,
            file_uuid: str,
            bundle_uuid: str = None,
            version: str = None,
            expected_code: int = requests.codes.created,
    ):
        bundle_uuid = str(uuid.uuid4()) if bundle_uuid is None else bundle_uuid
        if version is None:
            timestamp = datetime.datetime.utcnow()
            version = timestamp.strftime("%Y-%m-%dT%H%M%S.%fZ")

        urlbuilder = UrlBuilder().set(path='/v1/files/' + file_uuid)
        if version is not 'missing':
            urlbuilder.add_query("version", version)

        resp_obj = self.assertPutResponse(
            str(urlbuilder),
            expected_code,
            json_request_body=dict(
                bundle_uuid=bundle_uuid,
                creator_uid=0,
                source_url=source_url,
            ),
            headers=get_auth_header()
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
