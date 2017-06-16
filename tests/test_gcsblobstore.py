#!/usr/bin/env python
# coding: utf-8

from __future__ import absolute_import, division, print_function, unicode_literals

import os
import sys
import unittest

from test_blobstore import BlobStoreTests

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, pkg_root)

from dss.blobstore.gcs import GCSBlobStore # noqa


class TestGCSBlobStore(unittest.TestCase, BlobStoreTests):
    def setUp(self):
        self.credentials = os.path.join(pkg_root, "gcs-credentials.json")
        if "DSS_GCS_TEST_BUCKET" not in os.environ:
            raise Exception("Please set the DSS_GCS_TEST_BUCKET environment variable")
        self.test_bucket = os.environ["DSS_GCS_TEST_BUCKET"]
        self.test_src_data_bucket = os.environ["DSS_GCS_TEST_SRC_DATA_BUCKET"]
        self.handle = GCSBlobStore(self.credentials)

    def tearDown(self):
        pass

if __name__ == '__main__':
    unittest.main()
