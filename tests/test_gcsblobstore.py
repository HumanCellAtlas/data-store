#!/usr/bin/env python
# coding: utf-8

from __future__ import absolute_import, division, print_function, unicode_literals

import os
import sys
import unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, pkg_root)

from dss.blobstore.gcs import GCSBlobStore # noqa
from tests import utils # noqa
from tests.test_blobstore import BlobStoreTests # noqa


class TestGCSBlobStore(unittest.TestCase, BlobStoreTests):
    def setUp(self):
        self.credentials = os.path.join(pkg_root, "gcs-credentials.json")
        self.test_bucket = utils.get_env("DSS_GCS_TEST_BUCKET")
        self.handle = GCSBlobStore(self.credentials)

    def tearDown(self):
        pass

if __name__ == '__main__':
    unittest.main()
