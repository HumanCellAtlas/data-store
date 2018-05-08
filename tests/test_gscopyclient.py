#!/usr/bin/env python
# coding: utf-8

from __future__ import absolute_import, division, print_function, unicode_literals

import io
import os
import sys
import typing
import unittest
import uuid

from cloud_blobstore import BlobNotFoundError
from cloud_blobstore.gs import GSBlobStore

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss import BucketConfig, Config, Replica, stepfunctions
from dss.stepfunctions.gscopyclient import copy_sfn_event
from tests import eventually, infra
from tests.infra import testmode


class TestGSCopy(unittest.TestCase):
    def setUp(self, rounds=3):
        Config.set_config(BucketConfig.TEST)

        self.test_bucket = infra.get_env("DSS_GS_BUCKET_TEST")
        self.gs_blobstore = Config.get_blobstore_handle(Replica.gcp)
        test_src_keys = [infra.generate_test_key() for _ in range(rounds)]
        final_key = infra.generate_test_key()

        bucket_obj = self.gs_blobstore.gcp_client.bucket(self.test_bucket)

        self.gs_blobstore.upload_file_handle(
            self.test_bucket,
            test_src_keys[0],
            io.BytesIO(os.urandom(1024 * 1024))
        )

        for ix in range(len(test_src_keys) - 1):
            src_blob_obj = bucket_obj.get_blob(test_src_keys[ix])
            blobs = [src_blob_obj for _ in range(16)]
            dst_blob_obj = bucket_obj.blob(test_src_keys[ix + 1])

            dst_blob_obj.content_type = "application/octet-stream"
            dst_blob_obj.compose(blobs)

        # set the storage class to nearline.
        # NOTE: compose(…) does not seem to support setting a storage class.  The canonical way of changing storage
        # class is to call update_storage_class(…), but Google's libraries does not seem to handle
        # update_storage_class(…) calls for large objects.
        # Second NOTE: nearline storage upload is not as snappy as normal storage, and requires longer timeouts.
        final_blob_obj = Config.get_native_handle(
            Replica.gcp,
            connect_timeout=60,
            read_timeout=60,
        ).bucket(self.test_bucket).blob(final_key)
        final_blob_obj.storage_class = "NEARLINE"
        final_blob_src = bucket_obj.get_blob(test_src_keys[-1])
        token = None
        while True:
            result = final_blob_obj.rewrite(final_blob_src, token=token)
            if result[0] is None:
                # done!
                break
            token = result[0]

        self.src_key = final_key

    @testmode.integration
    def test_simple_copy(self):
        dest_key = infra.generate_test_key()

        state = copy_sfn_event(self.test_bucket, self.src_key, self.test_bucket, dest_key)
        execution_id = str(uuid.uuid4())
        stepfunctions.step_functions_invoke("dss-gs-copy-sfn-{stage}", execution_id, state)

        # verify that the destination has the same checksum.
        src_checksum = self.gs_blobstore.get_cloud_checksum(self.test_bucket, self.src_key)

        @eventually(30.0, 1.0, {BlobNotFoundError, AssertionError})
        def test_output():
            dst_checksum = self.gs_blobstore.get_cloud_checksum(self.test_bucket, dest_key)
            self.assertEqual(src_checksum, dst_checksum)

        test_output()


if __name__ == '__main__':
    unittest.main()
