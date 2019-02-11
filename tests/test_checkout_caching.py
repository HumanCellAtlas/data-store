#!/usr/bin/env python
# coding: utf-8
import os
import sys
import unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
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
        dss.Config.set_config(dss.BucketConfig.TEST)

    @testmode.standalone
    def test_aws_normal_checkout_creates_tag(self):
        """
        Verifies object level tagging of short-lived files.

        The current life-cycle policy that regularly deletes files in the AWS checkout bucket
        only applies to these tagged files.
        """
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
        """Verifies that long-lived Google cached objects are of the DURABLE_REDUCED_AVAILABILITY type."""
        pass

    @testmode.standalone
    def test_google_normal_checkout_creates_durable_storage_type(self):
        """
        Verifies object level tagging of short-lived files.

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
