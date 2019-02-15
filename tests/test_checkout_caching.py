#!/usr/bin/env python
# coding: utf-8
import os
import sys
import unittest
import tempfile
from google.cloud import storage

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss import Config, Replica, BucketConfig
from dss.stepfunctions import s3copyclient, gscopyclient
from tests import infra
from tests.infra import DSSAssertMixin, DSSUploadMixin, testmode
from tests.infra.server import ThreadedLocalServer


class TestCheckoutApi(unittest.TestCase, DSSAssertMixin, DSSUploadMixin):
    test_bundle_uploaded: bool = False
    bundle_uuid: str
    bundle_version: str
    file_uuid: str
    file_version: str

    class SpoofContext:
        @staticmethod
        def get_remaining_time_in_millis():
            return 2000

    @classmethod
    def setUpClass(cls):
        cls.app = ThreadedLocalServer()
        cls.app.start()

    @classmethod
    def tearDownClass(cls):
        cls.app.shutdown()

    def setUp(self):
        Config.set_config(BucketConfig.TEST)

    @testmode.standalone
    def test_aws_uncached_checkout_creates_tag(self):
        """
        Uncached files are tagged with {"uncached":"True"}
        This identifies them to be deleted by TTL rules.
        """
        src_data = os.urandom(1024)
        tagging = self._test_aws_cache(src_data, 'binary/octet')
        self.assertIn('uncached', tagging.keys())

    @testmode.standalone
    def test_aws_cached_checkout_doesnt_create_tag(self):
        """
        Cached files do not have any tagging on AWS.
        """
        src_data = os.urandom(1024)
        tagging = self._test_aws_cache(src_data, 'application/json')
        self.assertNotIn('uncached', tagging.keys())

    def test_aws_user_checkout_doesnt_create_tag(self):
        """
        Ensures that data is only cached when in a DSS-controlled bucket
        """
        src_data = os.urandom(1024)
        # cached data check
        tagging = self._test_aws_cache(src_data, 'application/json',
                                       checkout_bucket=os.environ['DSS_S3_CHECKOUT_BUCKET_TEST_USER'])
        self.assertNotIn('uncached', tagging.keys())
        # uncached data check
        tagging = self._test_aws_cache(src_data, 'binary/octet',
                                       checkout_bucket=os.environ['DSS_S3_CHECKOUT_BUCKET_TEST_USER'])
        self.assertNotIn('uncached', tagging.keys())

    def _test_aws_cache(self, src_data: bytes, content_type: str, checkout_bucket: str = None):
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
        event = s3copyclient.implementation.copy_worker(event, spoof_context, 1)
        # verify
        tagging = s3_blobstore.get_user_metadata(checkout_bucket, test_dst_key)
        # cleanup
        s3_blobstore.delete(replica.bucket, test_src_key)
        s3_blobstore.delete(checkout_bucket, test_dst_key)
        return tagging

    @testmode.standalone
    def test_google_cached_checkout_creates_multiregional_storage_type(self):
        """
        Verifies that long-lived Google cached objects are of the STANDARD type.
        MULTI_REGIONAL is an alias for STANDARD type
        """
        src_data = os.urandom(1024)
        blob_type = self._test_gs_cache(src_data, 'application/json')
        self.assertEqual('MULTI_REGIONAL', blob_type)

    @testmode.standalone
    def test_google_uncached_checkout_creates_durable_storage_type(self):
        """
                Verifies object level tagging of short-lived files.
                Verifies that short-lived Google cached objects are of the DURABLE_REDUCED_AVAILABILITY type.
        """
        src_data = os.urandom(1024)
        blob_type = self._test_gs_cache(src_data, 'binary/octet')
        self.assertEqual('DURABLE_REDUCED_AVAILABILITY', blob_type)

    def test_google_user_checkout_creates_multiregional_storage_type(self):
        """
        Ensures that both cached and uncached data is of the MULTI_REGIONAL type when
        checked out to a user's bucket, since we don't want to mess about with the user's data
        unnecessarily when they don't need caching.
        """
        # Note: The STANDARD storage type is also an alias for MULTI_REGIONAL.
        src_data = os.urandom(1024)
        # cached data check
        blob_type = self._test_gs_cache(src_data, 'application/json',
                                        checkout_bucket=os.environ['DSS_GS_CHECKOUT_BUCKET_TEST_USER'])
        self.assertEqual('MULTI_REGIONAL', blob_type)
        # uncached data check
        blob_type = self._test_gs_cache(src_data, 'binary/octet',
                                        checkout_bucket=os.environ['DSS_GS_CHECKOUT_BUCKET_TEST_USER'])
        self.assertEqual('MULTI_REGIONAL', blob_type)

    def _test_gs_cache(self, src_data: bytes, content_type: str, checkout_bucket: str = None):
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
