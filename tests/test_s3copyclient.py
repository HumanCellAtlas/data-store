#!/usr/bin/env python
# coding: utf-8

from __future__ import absolute_import, division, print_function, unicode_literals

import io
import itertools
import os
import sys
import unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.blobstore.s3 import S3BlobStore
from dss.events.chunkedtask.s3copyclient import S3CopyTask
from tests import infra
from tests.chunked_worker import TestStingyRuntime, run_task_to_completion


class TestAWSCopy(unittest.TestCase):
    def setUp(self):
        self.test_bucket = infra.get_env("DSS_S3_BUCKET_TEST")
        self.s3_blobstore = S3BlobStore()
        self.test_src_key = infra.generate_test_key()
        mpu = self.s3_blobstore.s3_client.create_multipart_upload(Bucket=self.test_bucket, Key=self.test_src_key)

        parts = list()
        for part_to_upload in range(1, 5):
            etag = self.s3_blobstore.s3_client.upload_part(
                Bucket=self.test_bucket,
                Key=self.test_src_key,
                UploadId=mpu['UploadId'],
                PartNumber=part_to_upload,
                ContentLength=(5 * 1024 * 1024),
                Body=chr(part_to_upload) * (5 * 1024 * 1024))['ETag']
            parts.append(dict(ETag=etag, PartNumber=part_to_upload))

        self.test_src_etag = self.s3_blobstore.s3_client.complete_multipart_upload(
            Bucket=self.test_bucket,
            Key=self.test_src_key,
            MultipartUpload=dict(Parts=parts),
            UploadId=mpu['UploadId'],
        )['ETag'].strip("\"")

    def test_simple_copy(self):
        dest_key = infra.generate_test_key()

        initial_state = S3CopyTask.setup_copy_task(
            self.test_bucket, self.test_src_key, self.test_bucket, dest_key, lambda blob_size: (5 * 1024 * 1024))

        freezes, _ = run_task_to_completion(
            S3CopyTask,
            initial_state,
            lambda results: TestStingyRuntime(results),
            lambda task_class, task_state, runtime: task_class(task_state),
            lambda runtime: runtime.result,
            lambda runtime: runtime.scheduled_work,
        )

        self.assertGreater(freezes, 0)

        # verify that the destination has the same checksum.
        dst_etag = S3BlobStore().get_all_metadata(self.test_bucket, dest_key)['ETag'].strip("\"")
        self.assertEqual(self.test_src_etag, dst_etag)

    def test_off_by_one(self):
        dest_key = infra.generate_test_key()

        initial_state = S3CopyTask.setup_copy_task(
            self.test_bucket, self.test_src_key, self.test_bucket, dest_key, lambda blob_size: (5 * 1024 * 1024))

        freezes, _ = run_task_to_completion(
            S3CopyTask,
            initial_state,
            lambda results: TestStingyRuntime(results, seq=itertools.repeat(sys.maxsize, 7)),
            lambda task_class, task_state, runtime: task_class(task_state, fetch_size=4),
            lambda runtime: runtime.result,
            lambda runtime: runtime.scheduled_work,
        )

        self.assertGreater(freezes, 0)

        # verify that the destination has the same checksum.
        dst_etag = S3BlobStore().get_all_metadata(self.test_bucket, dest_key)['ETag'].strip("\"")
        self.assertEqual(self.test_src_etag, dst_etag)


class TestAWSCopyNonMultipart(unittest.TestCase):
    def setUp(self):
        self.test_bucket = infra.get_env("DSS_S3_BUCKET_TEST")
        self.s3_blobstore = S3BlobStore()
        self.test_src_key = infra.generate_test_key()
        self.s3_blobstore.upload_file_handle(
            self.test_bucket,
            self.test_src_key,
            io.BytesIO(b"abcabcabc"))

        self.test_src_etag = self.s3_blobstore.get_cloud_checksum(self.test_bucket, self.test_src_key)

    def test_simple_copy(self):
        dest_key = infra.generate_test_key()

        initial_state = S3CopyTask.setup_copy_task(
            self.test_bucket, self.test_src_key, self.test_bucket, dest_key, lambda blob_size: (5 * 1024 * 1024))

        run_task_to_completion(
            S3CopyTask,
            initial_state,
            lambda results: TestStingyRuntime(results),
            lambda task_class, task_state, runtime: task_class(task_state),
            lambda runtime: runtime.result,
            lambda runtime: runtime.scheduled_work,
        )

        # verify that the destination has the same checksum.
        dst_etag = S3BlobStore().get_all_metadata(self.test_bucket, dest_key)['ETag'].strip("\"")
        self.assertEqual(self.test_src_etag, dst_etag)


if __name__ == '__main__':
    unittest.main()
