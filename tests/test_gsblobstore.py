#!/usr/bin/env python
# coding: utf-8

import os
import sys
import unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.blobstore.gs import GSBlobStore
from dss.blobstore import BlobNotFoundError
from tests import infra
from tests.test_blobstore import BlobStoreTests


class TestGSBlobStore(unittest.TestCase, BlobStoreTests):
    def setUp(self):
        self.credentials = infra.get_env("GOOGLE_APPLICATION_CREDENTIALS")
        self.test_bucket = infra.get_env("DSS_GS_BUCKET_TEST")
        self.test_fixtures_bucket = infra.get_env("DSS_GS_BUCKET_TEST_FIXTURES")
        self.handle = GSBlobStore(self.credentials)

    def tearDown(self):
        pass

    def test_get_checksum(self):
        """
        Ensure that the ``get_metadata`` methods return sane data.
        """
        handle = self.handle  # type: BlobStore
        checksum = handle.get_cloud_checksum(
            self.test_fixtures_bucket,
            "test_good_source_data/0")
        self.assertEqual(checksum, "e16e07b9")

        with self.assertRaises(BlobNotFoundError):
            handle.get_metadata(
                self.test_fixtures_bucket,
                "test_good_source_data_DOES_NOT_EXIST")

if __name__ == '__main__':
    unittest.main()
