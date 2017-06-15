#!/usr/bin/env python
# coding: utf-8

import os
import sys
import unittest
import uuid

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, pkg_root)

from dss.blobstore.s3 import S3BlobStore # noqa
from dss.hcablobstore.s3 import S3HCABlobStore # noqa
from tests import TESTOUTPUT_PREFIX # noqa

class TestS3HCABlobStore(unittest.TestCase):
    def setUp(self):
        if "DSS_S3_TEST_BUCKET" not in os.environ:
            raise Exception(
                "Please set the DSS_S3_TEST_BUCKET environment variable")
        if "DSS_S3_TEST_SRC_DATA_BUCKET" not in os.environ:
            raise Exception(
                "Please set the DSS_S3_TEST_SRC_DATA_BUCKET "
                "environment variable")
        self.test_src_data_container = os.environ["DSS_S3_TEST_SRC_DATA_BUCKET"]
        self.test_container = os.environ["DSS_S3_TEST_BUCKET"]
        self.blobhandle = S3BlobStore()
        self.hcahandle = S3HCABlobStore(self.blobhandle)

    def tearDown(self):
        pass

    def testCopy(self):
        function_name = self.testCopy.__name__
        dst_blob_name = os.path.join(
            TESTOUTPUT_PREFIX, function_name, str(uuid.uuid4()))
        self.hcahandle.copy_blob_from_staging(
            self.test_src_data_container, "test_good_source_data",
            self.test_container, dst_blob_name
        )
        src_metadata = self.blobhandle.get_metadata(
            self.test_src_data_container, "test_good_source_data")
        dst_metadata = self.blobhandle.get_metadata(
            self.test_container, dst_blob_name)
        self.assertEqual(src_metadata, dst_metadata)


if __name__ == '__main__':
    unittest.main()
