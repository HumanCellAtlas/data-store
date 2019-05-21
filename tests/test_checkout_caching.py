#!/usr/bin/env python
# coding: utf-8
import os
import sys
import unittest
import tempfile
from google.cloud import storage
from unittest import mock

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss import Config, Replica, BucketConfig
from dss.stepfunctions import s3copyclient, gscopyclient
from dss.storage.checkout.cache_flow import should_cache_file
from tests import infra
from tests.infra import DSSAssertMixin, DSSUploadMixin, testmode
from tests.infra.server import ThreadedLocalServer


class TestCheckoutCaching(unittest.TestCase, DSSAssertMixin, DSSUploadMixin):
    test_bundle_uploaded: bool = False
    bundle_uuid: str
    bundle_version: str
    file_uuid: str
    file_version: str

    class SpoofContext:
        @staticmethod
        def get_remaining_time_in_millis():
            return 11000  # This should be longer than 10 seconds to satisfy timing logic GS copy client sfn

    @classmethod
    def setUpClass(cls):
        cls.app = ThreadedLocalServer()
        cls.app.start()

    @classmethod
    def tearDownClass(cls):
        cls.app.shutdown()

    def setUp(self):
        Config.set_config(BucketConfig.TEST)

    def test_should_cache_file(self):
        tests = [('application/json; dcp-type=\"metadata/biomaterial\"', 1, True),
                 ('application/json', 1, True),
                 ('bapplication/json', 1, False),
                 ('application/json', 1e20, False),
                 ('binary/octet', 1, False)]
        for content_type, size, outcome in tests:
            with self.subTest("checkout should cache {content_type} {size} {outcome}"):
                self.assertEqual(should_cache_file(content_type, size), outcome)

    @mock.patch("dss.stepfunctions.s3copyclient.implementation.is_dss_bucket")
    @testmode.standalone
    def test_aws_uncached_checkout_creates_tag(self, mock_check):
        """
        Uncached files are tagged with {"uncached":"True"}
        This identifies them to be deleted by TTL rules.
        """
        mock_check.return_value = True
        src_data = os.urandom(1024)
        tagging = self._test_aws_cache(src_data=src_data, content_type='binary/octet',
                                       checkout_bucket=Replica.aws.checkout_bucket)
        self.assertIn('uncached', tagging.keys())

    @mock.patch("dss.stepfunctions.s3copyclient.implementation.is_dss_bucket")
    @testmode.standalone
    def test_aws_cached_checkout_doesnt_create_tag(self, mock_check):
        """
        Cached files do not have any tagging on AWS.
        """
        mock_check.return_value = True
        src_data = os.urandom(1024)
        tagging = self._test_aws_cache(src_data=src_data, content_type='application/json',
                                       checkout_bucket=Replica.aws.checkout_bucket)
        self.assertNotIn('uncached', tagging.keys())

    @testmode.integration
    def test_aws_user_checkout_doesnt_create_tag(self):
        """
        Ensures that data is only cached when in a DSS-controlled bucket
        """
        src_data = os.urandom(1024)
        # cached data check
        tagging = self._test_aws_cache(src_data=src_data, content_type='application/json',
                                       checkout_bucket=os.environ['DSS_S3_CHECKOUT_BUCKET_TEST_USER'])
        self.assertNotIn('uncached', tagging.keys())
        # uncached data check
        tagging = self._test_aws_cache(src_data=src_data, content_type='binary/octet',
                                       checkout_bucket=os.environ['DSS_S3_CHECKOUT_BUCKET_TEST_USER'])
        self.assertNotIn('uncached', tagging.keys())

    def _test_aws_cache(self, src_data, content_type, checkout_bucket):
        replica = Replica.aws
        checkout_bucket = checkout_bucket if checkout_bucket else replica.checkout_bucket
        test_src_key = infra.generate_test_key()
        s3_blobstore = Config.get_blobstore_handle(Replica.aws)
        # upload
        with tempfile.NamedTemporaryFile(delete=True) as fh:
            fh.write(src_data)
            fh.flush()
            fh.seek(0)
            s3_blobstore.upload_file_handle(replica.bucket, test_src_key, fh, content_type)
        # checkout
        test_dst_key = infra.generate_test_key()
        event = s3copyclient.copy_sfn_event(
            replica.bucket, test_src_key,
            checkout_bucket, test_dst_key)
        event = s3copyclient.implementation.setup_copy_task(event, None)
        spoof_context = self.SpoofContext()
        # parameters of copy_worker are arbitrary, only passed because required.
        event = s3copyclient.implementation.copy_worker(event, spoof_context, 10)
        # verify
        tagging = s3_blobstore.get_user_metadata(checkout_bucket, test_dst_key)
        # cleanup
        s3_blobstore.delete(replica.bucket, test_src_key)
        s3_blobstore.delete(checkout_bucket, test_dst_key)
        return tagging

    @mock.patch("dss.stepfunctions.gscopyclient.implementation.is_dss_bucket")
    @testmode.standalone
    def test_google_cached_checkout_creates_multiregional_storage_type(self, mock_check):
        """
        Verifies that long-lived Google cached objects are of the STANDARD type.
        MULTI_REGIONAL is an alias for STANDARD type
        """
        mock_check.return_value = True
        src_data = os.urandom(1024)
        blob_type = self._test_gs_cache(src_data=src_data, content_type='application/json',
                                        checkout_bucket=Replica.gcp.checkout_bucket)
        self.assertEqual('MULTI_REGIONAL', blob_type)

    @mock.patch("dss.stepfunctions.gscopyclient.implementation.is_dss_bucket")
    @testmode.standalone
    def test_google_uncached_checkout_creates_standard_storage_type(self, mock_check):
        """
        Verifies object level tagging of short-lived files.
        Verifies that short-lived Google cached objects are of the STANDARD type.
        """
        mock_check.return_value = True
        src_data = os.urandom(1024)
        blob_type = self._test_gs_cache(src_data=src_data, content_type='binary/octet',
                                        checkout_bucket=Replica.gcp.checkout_bucket)
        self.assertEqual('STANDARD', blob_type)

    @testmode.integration
    def test_google_user_checkout_creates_multiregional_storage_type(self):
        """
        Ensures that both cached and uncached data is of the MULTI_REGIONAL type when
        checked out to a user's bucket, since we don't want to mess about with the user's data
        unnecessarily when they don't need caching.
        """
        # Note: The STANDARD storage type is also an alias for MULTI_REGIONAL.

        src_data = os.urandom(1024)
        # cached data check
        blob_type = self._test_gs_cache(src_data=src_data, content_type='application/json',
                                        checkout_bucket=os.environ['DSS_GS_CHECKOUT_BUCKET_TEST_USER'])
        self.assertEqual('MULTI_REGIONAL', blob_type)
        # uncached data check
        blob_type = self._test_gs_cache(src_data=src_data, content_type='binary/octet',
                                        checkout_bucket=os.environ['DSS_GS_CHECKOUT_BUCKET_TEST_USER'])
        self.assertEqual('MULTI_REGIONAL', blob_type)

    def _test_gs_cache(self, src_data, content_type, checkout_bucket):
        replica = Replica.gcp
        checkout_bucket = checkout_bucket if checkout_bucket else replica.checkout_bucket
        test_src_key = infra.generate_test_key()
        gs_blobstore = Config.get_blobstore_handle(Replica.gcp)
        client = storage.Client()
        # upload
        with tempfile.NamedTemporaryFile(delete=True) as fh:
            fh.write(src_data)
            fh.flush()
            fh.seek(0)
            gs_blobstore.upload_file_handle(replica.bucket, test_src_key, fh, content_type)
        # checkout
        test_dst_key = infra.generate_test_key()
        event = gscopyclient.copy_sfn_event(
            replica.bucket, test_src_key,
            checkout_bucket, test_dst_key)
        event = gscopyclient.implementation.setup_copy_task(event, None)
        spoof_context = self.SpoofContext()
        # parameters of copy_worker are arbitrary, only passed because required.
        event = gscopyclient.implementation.copy_worker(event, spoof_context)
        # verify
        bucket = client.get_bucket(checkout_bucket)
        blob_class = bucket.get_blob(test_dst_key).storage_class
        # cleanup
        gs_blobstore.delete(replica.bucket, test_src_key)
        gs_blobstore.delete(checkout_bucket, test_dst_key)
        return blob_class


if __name__ == "__main__":
    unittest.main()
