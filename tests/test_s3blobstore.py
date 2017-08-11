#!/usr/bin/env python
# coding: utf-8

import os
import sys
import unittest
import uuid

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.blobstore import BlobNotFoundError
from dss.blobstore.s3 import S3BlobStore
from tests import infra
from tests.test_blobstore import BlobStoreTests


class TestS3BlobStore(unittest.TestCase, BlobStoreTests):
    def setUp(self):
        self.test_bucket = infra.get_env("DSS_S3_BUCKET_TEST")
        self.test_fixtures_bucket = infra.get_env("DSS_S3_BUCKET_TEST_FIXTURES")

        self.handle = S3BlobStore()

    def tearDown(self):
        pass

    def test_get_checksum(self):
        """
        Ensure that the ``get_metadata`` methods return sane data.
        """
        handle = self.handle  # type: BlobStore
        checksum = handle.get_cloud_checksum(
            self.test_fixtures_bucket,
            "test_good_source_data/0")
        self.assertEqual(checksum, "3b83ef96387f14655fc854ddc3c6bd57")

        with self.assertRaises(BlobNotFoundError):
            handle.get_user_metadata(
                self.test_fixtures_bucket,
                "test_good_source_data_DOES_NOT_EXIST")

    def find_next_missing_parts_test_case(self, handle, parts_to_upload, *args, **kwargs):
        key = str(uuid.uuid4())
        mpu = handle.s3_client.create_multipart_upload(Bucket=self.test_bucket, Key=key)

        try:
            for part_to_upload in parts_to_upload:
                handle.s3_client.upload_part(
                    Bucket=self.test_bucket,
                    Key=key,
                    UploadId=mpu['UploadId'],
                    PartNumber=part_to_upload,
                    Body=f"part{part_to_upload:05}".encode("utf-8"))

            return handle.find_next_missing_parts(self.test_bucket, key, mpu['UploadId'], *args, **kwargs)
        finally:
            handle.s3_client.abort_multipart_upload(Bucket=self.test_bucket, Key=key, UploadId=mpu['UploadId'])

    def test_find_next_missing_parts_simple(self):
        handle = self.handle  # type: BlobStore

        # simple test case, 2 parts, 1 part uploaded.
        res = self.find_next_missing_parts_test_case(handle, [1], part_count=2)
        self.assertEqual(res, [2])

        res = self.find_next_missing_parts_test_case(handle, [1], part_count=2, search_start=1)
        self.assertEqual(res, [2])

        res = self.find_next_missing_parts_test_case(handle, [1], part_count=2, search_start=1, return_count=2)
        self.assertEqual(res, [2])

        res = self.find_next_missing_parts_test_case(handle, [1], part_count=2, search_start=2)
        self.assertEqual(res, [2])

        res = self.find_next_missing_parts_test_case(handle, [1], part_count=2, search_start=2, return_count=2)
        self.assertEqual(res, [2])

        with self.assertRaises(ValueError):
            self.find_next_missing_parts_test_case(handle, [1], part_count=2, search_start=3, return_count=2)

    def test_find_next_missing_parts_multiple_requests(self):
        handle = self.handle  # type: BlobStore

        # 10000 parts, one is uploaded.
        res = self.find_next_missing_parts_test_case(handle, [1], part_count=10000)
        self.assertEqual(res, [2])

        # 10000 parts, one is uploaded, get all the missing parts.
        res = self.find_next_missing_parts_test_case(handle, [1], part_count=10000, return_count=10000)
        self.assertEqual(len(res), 9999)
        self.assertNotIn(1, res)

        # 10000 parts, one is uploaded, get all the missing parts.
        res = self.find_next_missing_parts_test_case(handle, [1], part_count=10000, return_count=1000)
        self.assertEqual(len(res), 1000)
        self.assertNotIn(1, res)

        # 10000 parts, one is uploaded, get all the missing parts.
        res = self.find_next_missing_parts_test_case(handle, [1], part_count=10000, search_start=100, return_count=1000)
        self.assertEqual(len(res), 1000)
        self.assertNotIn(1, res)
        for missing_part in res:
            self.assertGreaterEqual(missing_part, 100)

        # 10000 parts, all the parts numbers divisible by 2000 is uploaded, get all the missing parts.
        res = self.find_next_missing_parts_test_case(
            handle,
            [ix
             for ix in range(1, 10000 + 1)
             if ix % 2000 == 0],
            part_count=10000,
            return_count=10000)
        self.assertEqual(len(res), 9995)
        for ix in range(1, 10000 + 1):
            if ix % 2000 == 0:
                self.assertNotIn(ix, res)
            else:
                self.assertIn(ix, res)

        # 10000 parts, all the parts numbers divisible by 2000 is uploaded, get all the missing parts starting at part
        # 1001.
        res = self.find_next_missing_parts_test_case(
            handle,
            [ix
             for ix in range(1, 10000 + 1)
             if ix % 2000 == 0],
            part_count=10000,
            search_start=1001,
            return_count=10000)
        self.assertEqual(len(res), 8995)
        for ix in range(1001, 10000 + 1):
            if ix % 2000 == 0:
                self.assertNotIn(ix, res)
            else:
                self.assertIn(ix, res)

if __name__ == '__main__':
    unittest.main()
