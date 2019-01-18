#!/usr/bin/env python
# coding: utf-8
import io
import os
import sys
import unittest

import botocore
from cloud_blobstore import BlobNotFoundError
from cloud_blobstore.s3 import S3BlobStore

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.storage.hcablobstore.s3 import S3HCABlobStore
from tests import infra
from tests.hcablobstore_base import HCABlobStoreTests


class TestS3HCABlobStore(unittest.TestCase, HCABlobStoreTests):
    def setUp(self):
        self.bucket = infra.get_env("DSS_S3_BUCKET")
        self.test_bucket = infra.get_env("DSS_S3_BUCKET_TEST")
        self.test_fixtures_bucket = infra.get_env("DSS_S3_BUCKET_TEST_FIXTURES")
        self.blobhandle = S3BlobStore.from_environment()
        self.hcahandle = S3HCABlobStore(self.blobhandle)

    def tearDown(self):
        pass

    def test_delete_object(self):
        file_name = 'Deletion_test.txt'
        # reupload the test file if it was deleted
        try:
            self.blobhandle.get_content_type(self.bucket, file_name)
        except BlobNotFoundError:
            fobj = io.BytesIO(b"This should never be deleted by a test.")
            self.blobhandle.upload_file_handle(self.bucket, file_name, fobj)

        # Try deleting the file from the replica. This should fail.
        try:
            self.blobhandle.delete(self.bucket, file_name)
        except botocore.exceptions.ClientError as ex:
            self.assertEqual(ex.response["Error"]["Code"], '403')
        else:
            self.fail(f"Excepted 403")


if __name__ == '__main__':
    unittest.main()
