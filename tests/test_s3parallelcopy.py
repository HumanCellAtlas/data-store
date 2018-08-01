#!/usr/bin/env python
# coding: utf-8

import os
import sys
import tempfile
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor

import boto3
import botocore
from dcplib.s3_multipart import AWS_MIN_CHUNK_SIZE

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss import Config, Replica, stepfunctions
from dss.stepfunctions import s3copyclient
from dss.stepfunctions.s3copyclient.implementation import LAMBDA_PARALLELIZATION_FACTOR
from tests import eventually, infra
from tests.infra import testmode


class TestS3ParallelCopy(unittest.TestCase):
    @staticmethod
    def upload_part(bucket: str, key: str, upload_id: str, part_id: int):
        s3_client = boto3.client("s3")
        etag = s3_client.upload_part(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            PartNumber=part_id,
            ContentLength=AWS_MIN_CHUNK_SIZE,
            Body=chr(part_id % 256) * AWS_MIN_CHUNK_SIZE)['ETag']
        return part_id, etag

    @eventually(30 * 60, 5.0, {AssertionError, botocore.exceptions.ClientError})
    def _check_dst_key_etag(self, bucket: str, key: str, expected_etag: str):
        s3_client = boto3.client("s3")
        obj_metadata = s3_client.head_object(
            Bucket=bucket,
            Key=key
        )
        self.assertEqual(expected_etag, obj_metadata['ETag'].strip("\""))

    @testmode.integration
    def test_zero_copy(self):
        test_bucket = infra.get_env("DSS_S3_BUCKET_TEST")
        test_src_key = infra.generate_test_key()
        s3_blobstore = Config.get_blobstore_handle(Replica.aws)

        with tempfile.NamedTemporaryFile(delete=True) as fh:
            fh.seek(0)
            s3_blobstore.upload_file_handle(test_bucket, test_src_key, fh)

        src_etag = s3_blobstore.get_cloud_checksum(test_bucket, test_src_key)

        test_dst_key = infra.generate_test_key()
        state = s3copyclient.copy_sfn_event(
            test_bucket, test_src_key,
            test_bucket, test_dst_key)
        execution_id = str(uuid.uuid4())
        stepfunctions.step_functions_invoke("dss-s3-copy-sfn-{stage}", execution_id, state)

        self._check_dst_key_etag(test_bucket, test_dst_key, src_etag)

    @testmode.integration
    def test_tiny_copy(self):
        test_bucket = infra.get_env("DSS_S3_BUCKET_TEST")
        test_src_key = infra.generate_test_key()
        src_data = os.urandom(1024)
        s3_blobstore = Config.get_blobstore_handle(Replica.aws)

        with tempfile.NamedTemporaryFile(delete=True) as fh:
            fh.write(src_data)
            fh.flush()
            fh.seek(0)
            s3_blobstore.upload_file_handle(test_bucket, test_src_key, fh)

        src_etag = s3_blobstore.get_cloud_checksum(test_bucket, test_src_key)

        test_dst_key = infra.generate_test_key()
        state = s3copyclient.copy_sfn_event(
            test_bucket, test_src_key,
            test_bucket, test_dst_key)
        execution_id = str(uuid.uuid4())
        stepfunctions.step_functions_invoke("dss-s3-copy-sfn-{stage}", execution_id, state)

        self._check_dst_key_etag(test_bucket, test_dst_key, src_etag)

    @testmode.integration
    def test_large_copy(self, num_parts=LAMBDA_PARALLELIZATION_FACTOR + 1):
        test_bucket = infra.get_env("DSS_S3_BUCKET_TEST")
        test_src_key = infra.generate_test_key()
        s3_client = boto3.client("s3")
        mpu = s3_client.create_multipart_upload(Bucket=test_bucket, Key=test_src_key)

        with ThreadPoolExecutor(max_workers=8) as tpe:
            parts_futures = tpe.map(
                lambda part_id: TestS3ParallelCopy.upload_part(test_bucket, test_src_key, mpu['UploadId'], part_id),
                range(1, num_parts + 1))

        parts = [dict(ETag=part_etag, PartNumber=part_id) for part_id, part_etag in parts_futures]

        src_etag = s3_client.complete_multipart_upload(
            Bucket=test_bucket,
            Key=test_src_key,
            MultipartUpload=dict(Parts=parts),
            UploadId=mpu['UploadId'],
        )['ETag'].strip('"')

        test_dst_key = infra.generate_test_key()
        state = s3copyclient.copy_sfn_event(
            test_bucket, test_src_key,
            test_bucket, test_dst_key)
        execution_id = str(uuid.uuid4())
        stepfunctions.step_functions_invoke("dss-s3-copy-sfn-{stage}", execution_id, state)

        self._check_dst_key_etag(test_bucket, test_dst_key, src_etag)


if __name__ == '__main__':
    unittest.main()
