#!/usr/bin/env python
# coding: utf-8

import os
import sys
import unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..')) # noqa
sys.path.insert(0, pkg_root) # noqa

from dss.blobstore.s3 import S3BlobStore
from dss.hcablobstore.s3 import S3HCABlobStore
from tests import infra
from tests.test_hcablobstore import HCABlobStoreTests


class TestS3HCABlobStore(unittest.TestCase, HCABlobStoreTests):
    def setUp(self):
        self.test_bucket = infra.get_env("DSS_S3_TEST_BUCKET")
        self.test_fixtures_bucket = infra.get_env("DSS_S3_TEST_FIXTURES_BUCKET")
        self.blobhandle = S3BlobStore()
        self.hcahandle = S3HCABlobStore(self.blobhandle)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
