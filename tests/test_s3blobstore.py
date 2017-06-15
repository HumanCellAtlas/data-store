#!/usr/bin/env python
# coding: utf-8

from __future__ import absolute_import, division, print_function, unicode_literals

import io
import os
import sys
import unittest
import uuid


pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, pkg_root)

from dss.blobstore.s3 import S3BlobStore # noqa
from tests import TESTOUTPUT_PREFIX # noqa
from tests.test_blobstore import BlobStoreTests # noqa


class TestS3BlobStore(unittest.TestCase, BlobStoreTests):
    def setUp(self):
        if "DSS_S3_TEST_BUCKET" not in os.environ:
            raise Exception("Please set the DSS_S3_TEST_BUCKET environment variable")
        self.test_bucket = os.environ["DSS_S3_TEST_BUCKET"]
        self.handle = S3BlobStore()

    def tearDown(self):
        pass

    # TODO: this should be moved to BlobStoreTests once we build the GCS
    # equivalents out
    def testUploadFileHandle(self):
        fobj = io.BytesIO(b"abcabcabc")
        function_name = "%s.%s" % (
            TestS3BlobStore.__name__,
            self.testUploadFileHandle.__name__
        )
        dst_blob_name = os.path.join(
            TESTOUTPUT_PREFIX, function_name, str(uuid.uuid4()))

        self.handle.upload_file_handle(
            self.test_bucket,
            dst_blob_name,
            fobj
        )

        # should be able to get metadata for the file.
        self.handle.get_metadata(
            self.test_bucket, dst_blob_name)

if __name__ == '__main__':
    unittest.main()
