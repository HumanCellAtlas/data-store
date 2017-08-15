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
from dss.events import chunkedtask
from dss.events.chunkedtask.awscopyclient import S3CopyTask
from tests import infra


class TestStingyRuntime(chunkedtask.Runtime[dict, bool]):
    """This is runtime that returns a pre-determined sequence, and then 0s for the remaining time."""
    def __init__(self, seq=None):
        self.complete = False
        if seq is None:
            seq = list()
        self.seq = itertools.chain(seq, itertools.repeat(0))

    def get_remaining_time_in_millis(self) -> int:
        return self.seq.__next__()

    def schedule_work(self, state: dict):
        # it's illegal for there to be no state.
        assert state is not None
        self.rescheduled_state = state

    def work_complete_callback(self, bool):
        self.complete = True


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

        current_state = S3CopyTask.setup_copy_task(
            self.test_bucket, self.test_src_key, self.test_bucket, dest_key, lambda blob_size: (5 * 1024 * 1024))

        while True:
            env = TestStingyRuntime()
            task = S3CopyTask(current_state)
            runner = chunkedtask.Runner(task, env)

            runner.run()

            if env.complete:
                # we're done!
                break
            else:
                current_state = env.rescheduled_state

        # verify that the destination has the same checksum.
        dst_etag = S3BlobStore().get_all_metadata(self.test_bucket, dest_key)['ETag'].strip("\"")
        self.assertEqual(self.test_src_etag, dst_etag)

    def test_off_by_one(self):
        dest_key = infra.generate_test_key()

        current_state = S3CopyTask.setup_copy_task(
            self.test_bucket, self.test_src_key, self.test_bucket, dest_key, lambda blob_size: (5 * 1024 * 1024))

        while True:
            env = TestStingyRuntime(seq=itertools.repeat(sys.maxsize, 9))
            task = S3CopyTask(current_state, fetch_size=4)
            runner = chunkedtask.Runner(task, env)

            runner.run()

            if env.complete:
                # we're done!
                break
            else:
                current_state = env.rescheduled_state

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

        current_state = S3CopyTask.setup_copy_task(
            self.test_bucket, self.test_src_key, self.test_bucket, dest_key, lambda blob_size: (5 * 1024 * 1024))

        while True:
            env = TestStingyRuntime()
            task = S3CopyTask(current_state)
            runner = chunkedtask.Runner(task, env)

            runner.run()

            if env.complete:
                # we're done!
                break
            else:
                current_state = env.rescheduled_state

        # verify that the destination has the same checksum.
        dst_etag = S3BlobStore().get_all_metadata(self.test_bucket, dest_key)['ETag'].strip("\"")
        self.assertEqual(self.test_src_etag, dst_etag)


if __name__ == '__main__':
    unittest.main()
