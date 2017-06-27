#!/usr/bin/env python
# coding: utf-8

import os
import sys
import unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..')) # noqa
sys.path.insert(0, pkg_root) # noqa

from dss.blobstore import BlobNotFoundError
from dss.blobstore.gcs import GCSBlobStore
from dss.hcablobstore import HCABlobStore
from dss.hcablobstore.gcs import GCSHCABlobStore
from tests import utils


class TestGCSHCABlobStore(unittest.TestCase):
    def setUp(self):
        self.credentials = os.path.join(pkg_root, "gcs-credentials.json")
        self.test_bucket = utils.get_env("DSS_GCS_TEST_BUCKET")
        self.test_src_data_bucket = utils.get_env("DSS_GCS_TEST_SRC_DATA_BUCKET")
        self.blobhandle = GCSBlobStore(self.credentials)
        self.hcahandle = GCSHCABlobStore(self.blobhandle)

    def tearDown(self):
        pass

    def test_verify_blob_checksum(self):
        self.assertTrue(
            self.hcahandle.verify_blob_checksum(
                self.test_src_data_bucket, "test_good_source_data/0",
                {
                    HCABlobStore.MANDATORY_METADATA['CRC32C']['keyname']: "e16e07b9",
                }
            )
        )

        self.assertFalse(
            self.hcahandle.verify_blob_checksum(
                self.test_src_data_bucket, "test_good_source_data/1",
                {
                    HCABlobStore.MANDATORY_METADATA['CRC32C']['keyname']: "e16e07b9",
                }
            )
        )

        with self.assertRaises(BlobNotFoundError):
            self.hcahandle.verify_blob_checksum(
                self.test_src_data_bucket, "DOES_NOT_EXIST",
                {
                    HCABlobStore.MANDATORY_METADATA['CRC32C']['keyname']: "e16e07b9",
                }
            )


if __name__ == '__main__':
    unittest.main()
