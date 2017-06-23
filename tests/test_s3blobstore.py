#!/usr/bin/env python
# coding: utf-8

from __future__ import absolute_import, division, print_function, unicode_literals

import os
import sys
import unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, pkg_root)

from dss.blobstore import BlobNotFoundError # noqa
from dss.blobstore.s3 import S3BlobStore # noqa
from tests import utils # noqa
from tests.test_blobstore import BlobStoreTests # noqa


class TestS3BlobStore(unittest.TestCase, BlobStoreTests):
    def setUp(self):
        self.test_bucket = utils.get_env("DSS_S3_TEST_BUCKET")
        self.test_src_data_bucket = utils.get_env("DSS_S3_TEST_SRC_DATA_BUCKET")

        self.handle = S3BlobStore()

    def tearDown(self):
        pass

    def test_get_checksum(self):
        """
        Ensure that the ``get_metadata`` methods return sane data.
        """
        handle = self.handle  # type: BlobStore
        checksum = handle.get_cloud_checksum(
            self.test_src_data_bucket,
            "test_good_source_data/0")
        self.assertEqual(checksum, "3b83ef96387f14655fc854ddc3c6bd57")

        with self.assertRaises(BlobNotFoundError):
            handle.get_metadata(
                self.test_src_data_bucket,
                "test_good_source_data_DOES_NOT_EXIST")

if __name__ == '__main__':
    unittest.main()
