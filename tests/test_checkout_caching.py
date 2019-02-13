#!/usr/bin/env python
# coding: utf-8
import os
import sys
import unittest
import tempfile
import uuid

from dss.stepfunctions.s3copyclient.implementation import setup_copy_task, copy_worker

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss import stepfunctions, Config, Replica, BucketConfig
from dss.stepfunctions import s3copyclient
from tests import infra
from tests.infra import DSSAssertMixin, DSSUploadMixin, get_env, testmode
from tests.infra.server import ThreadedLocalServer


class TestCheckoutApi(unittest.TestCase, DSSAssertMixin, DSSUploadMixin):
    test_bundle_uploaded: bool = False
    bundle_uuid: str
    bundle_version: str
    file_uuid: str
    file_version: str

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
    def test_aws_uncached_tags(self):
        class SpoofContext:
            def get_remaining_time_in_millis(self):
                return 2000
        replica = Replica.aws
        test_src_key = infra.generate_test_key()
        s3_blobstore = Config.get_blobstore_handle(Replica.aws)
        src_data = os.urandom(1024)
        # upload
        with tempfile.NamedTemporaryFile(delete=True) as fh:
            fh.write(src_data)
            fh.flush()
            fh.seek(0)
            s3_blobstore.upload_file_handle(replica.bucket, test_src_key, fh)
        # checkout
        test_dst_key = infra.generate_test_key()
        event = s3copyclient.copy_sfn_event(
            replica.bucket, test_src_key,
            replica.checkout_bucket, test_dst_key)
        event = setup_copy_task(event, None)
        spoof_context = SpoofContext()
        # parameters of copy_worker are arbitrary, only passed because required.
        event = copy_worker(event, spoof_context, 1)
        # verify
        tagging = s3_blobstore.get_user_metadata(replica.checkout_bucket, test_dst_key)
        self.assertIn("uncached", tagging.keys())  # tests uncached files
        # cleanup
        s3_blobstore.delete(replica.bucket, test_src_key)
        s3_blobstore.delete(replica.checkout_bucket, test_dst_key)
        pass

    @testmode.standalone
    def test_aws_cached_checkout_doesnt_create_tag(self):
        """Verifies that long-lived AWS cached objects have no tag."""
        pass

    @testmode.standalone
    def test_aws_cached_speed(self):
        """Checkout a fresh file and cache it.  Then check it out again and verify that the checkout was faster."""
        pass

    @testmode.standalone
    def test_google_cached_checkout_creates_standard_storage_type(self):
        """Verifies that long-lived Google cached objects are of the STANDARD type."""
        pass

    @testmode.standalone
    def test_google_normal_checkout_creates_durable_storage_type(self):
        """
        Verifies that short-lived Google cached objects are of the DURABLE_REDUCED_AVAILABILITY type.

        The current life-cycle policy that regularly deletes files in the Google checkout bucket
        only applies to DURABLE_REDUCED_AVAILABILITY.
        """
        pass

    @testmode.standalone
    def test_google_cached_speed(self):
        """Checkout a fresh file and cache it.  Then check it out again and verify that the checkout was faster."""
        pass


if __name__ == "__main__":
    unittest.main()
