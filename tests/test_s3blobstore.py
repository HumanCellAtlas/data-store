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

from dss.blobstore import BlobNotFoundError # noqa
from dss.blobstore.s3 import S3BlobStore # noqa
from tests import TESTOUTPUT_PREFIX, utils # noqa
from tests.test_blobstore import BlobStoreTests # noqa


class TestS3BlobStore(unittest.TestCase, BlobStoreTests):
    def setUp(self):
        self.test_bucket = utils.get_env("DSS_S3_TEST_BUCKET")
        self.test_src_data_bucket = utils.get_env("DSS_S3_TEST_SRC_DATA_BUCKET")

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

    # TODO: this should be moved to BlobStoreTests once we build the GCS
    # equivalents out
    def testGet(self):
        data = self.handle.get(
            self.test_src_data_bucket,
            "test_good_source_data/0",
        )
        self.assertEqual(len(data), 11358)

        with self.assertRaises(BlobNotFoundError):
            self.handle.get(
                self.test_src_data_bucket,
                "test_good_source_data_DOES_NOT_EXIST",
            )

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
