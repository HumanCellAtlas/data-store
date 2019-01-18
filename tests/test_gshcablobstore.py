#!/usr/bin/env python
# coding: utf-8
import google
import io
import os
import sys
import unittest

from cloud_blobstore import BlobNotFoundError
from cloud_blobstore.gs import GSBlobStore

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.storage.hcablobstore.gs import GSHCABlobStore
from tests import infra
from tests.hcablobstore_base import HCABlobStoreTests


class TestGSHCABlobStore(unittest.TestCase, HCABlobStoreTests):
    def setUp(self):
        self.bucket = infra.get_env("DSS_GS_BUCKET")
        self.credentials = infra.get_env("GOOGLE_APPLICATION_CREDENTIALS")
        self.test_bucket = infra.get_env("DSS_GS_BUCKET_TEST")
        self.test_fixtures_bucket = infra.get_env("DSS_GS_BUCKET_TEST_FIXTURES")
        self.blobhandle = GSBlobStore.from_auth_credentials(self.credentials)
        self.hcahandle = GSHCABlobStore(self.blobhandle)

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
        except google.api_core.exceptions.Forbidden as ex:
            self.assertEqual(ex.response.status_code, 403)
        else:
            self.fail(f"Excepted 403")


if __name__ == '__main__':
    unittest.main()
