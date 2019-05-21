#!/usr/bin/env python
# coding: utf-8

import io
import os
import sys
import uuid
import json
import argparse
import unittest
from collections import namedtuple
from unittest import mock

from cloud_blobstore import BlobNotFoundError

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from tests.infra import testmode
from dss.operations import DSSOperationsCommandDispatch
from dss.operations.util import map_bucket_results, CommandForwarder
from dss.operations import storage
from dss.logging import configure_test_logging
from dss.config import BucketConfig, Config, Replica, override_bucket_config
from dss.storage.hcablobstore import FileMetadata, compose_blob_key

def setUpModule():
    configure_test_logging()

@testmode.standalone
class TestOperations(unittest.TestCase):
    def test_dispatch(self):
        with self.subTest("dispatch without mutually exclusive arguments"):
            self._test_dispatch()

        with self.subTest("dispatch with mutually exclusive arguments"):
            self._test_dispatch(mutually_exclusive=True)

        with self.subTest("dispatch with action overrides"):
            self._test_dispatch(action_overrides=True)

    def _test_dispatch(self, mutually_exclusive=None, action_overrides=False):
        dispatch = DSSOperationsCommandDispatch()
        target = dispatch.target(
            "my_target",
            arguments={
                "foo": dict(default="george", type=int),
                "--argument-a": None,
                "--argument-b": dict(default="bar"),
            },
            mutually_exclusive=(["--argument-a", "--argument-b"] if mutually_exclusive else None)
        )

        if action_overrides:
            @target.action("my_action", arguments={"foo": None, "--bar": dict(default="bars")})
            def my_action(argv, args):
                self.assertEqual(args.argument_b, "LSDKFJ")
                self.assertEqual(args.foo, "24")
                self.assertEqual(args.bar, "bars")
        else:
            @target.action("my_action")
            def my_action(argv, args):
                self.assertEqual(args.argument_b, "LSDKFJ")
                self.assertEqual(args.foo, 24)

        dispatch(["my_target", "my_action", "24", "--argument-b", "LSDKFJ"])

    def test_map_bucket(self):
        with override_bucket_config(BucketConfig.TEST_FIXTURE):
            for replica in Replica:
                with self.subTest(replica=replica):
                    handle = Config.get_blobstore_handle(replica)

                    count_list = 0
                    for key in handle.list(replica.bucket, prefix="bundles/"):
                        count_list += 1

                    def counter(keys):
                        count = 0
                        for key in keys:
                            count += 1
                        return count

                    total = 0
                    for count in map_bucket_results(counter, handle, replica.bucket, "bundles/", 2):
                        total += count

                    self.assertGreater(count_list, 0)
                    self.assertEqual(count_list, total)

    def test_command_forwarder(self):
        batches = list()

        def mock_enqueue(commands):
            batches.append(commands)

        with mock.patch("dss.operations.util._enqueue_command_batch", mock_enqueue):
            with CommandForwarder() as f:
                for i in range(21):
                    f.forward(str(i))

        self.assertEqual(batches[0], [str(i) for i in range(10)])
        self.assertEqual(batches[1], [str(i) for i in range(10, 20)])
        self.assertEqual(batches[2], [str(i) for i in range(20, 21)])

    def test_repair_blob_metadata(self):
        uploader = {Replica.aws: self._put_s3_file, Replica.gcp: self._put_gs_file}
        with override_bucket_config(BucketConfig.TEST):
            for replica in Replica:
                handle = Config.get_blobstore_handle(replica)
                key = str(uuid.uuid4())
                file_metadata = {
                    FileMetadata.SHA256: "foo",
                    FileMetadata.SHA1: "foo",
                    FileMetadata.S3_ETAG: "foo",
                    FileMetadata.CRC32C: "foo",
                    FileMetadata.CONTENT_TYPE: "foo"
                }
                blob_key = compose_blob_key(file_metadata)
                uploader[replica](key, json.dumps(file_metadata).encode("utf-8"), "application/json")
                uploader[replica](blob_key, b"123", "bar")
                args = argparse.Namespace(keys=[key], entity_type="files", job_id="", replica=replica.name)

                with self.subTest("Blob content type repaired", replica=replica):
                    storage.repair_blob_content_type([], args).process_key(key)
                    self.assertEqual(handle.get_content_type(replica.bucket, blob_key),
                                     file_metadata[FileMetadata.CONTENT_TYPE])

                with self.subTest("Should handle arbitrary exceptions", replica=replica):
                    with mock.patch("dss.operations.storage.StorageOperationHandler.log_error") as log_error:
                        with mock.patch("dss.config.Config.get_native_handle") as thrower:
                            thrower.side_effect = Exception()
                            storage.repair_blob_content_type([], args).process_key(key)
                            log_error.assert_called()
                            self.assertEqual(log_error.call_args[0][0], "Exception")

                with self.subTest("Should handle missing file metadata", replica=replica):
                    with mock.patch("dss.operations.storage.StorageOperationHandler.log_warning") as log_warning:
                        storage.repair_blob_content_type([], args).process_key("wrong key")
                        self.assertEqual(log_warning.call_args[0][0], "BlobNotFoundError")

                with self.subTest("Should handle missing blob", replica=replica):
                    with mock.patch("dss.operations.storage.StorageOperationHandler.log_warning") as log_warning:
                        file_metadata[FileMetadata.SHA256] = "wrong"
                        uploader[replica](key, json.dumps(file_metadata).encode("utf-8"), "application/json")
                        storage.repair_blob_content_type([], args).process_key(key)
                        self.assertEqual(log_warning.call_args[0][0], "BlobNotFoundError")

                with self.subTest("Should handle corrupt file metadata", replica=replica):
                    with mock.patch("dss.operations.storage.StorageOperationHandler.log_warning") as log_warning:
                        uploader[replica](key, b"this is not json", "application/json")
                        storage.repair_blob_content_type([], args).process_key(key)
                        self.assertEqual(log_warning.call_args[0][0], "JSONDecodeError")

    def test_update_content_type(self):
        TestCase = namedtuple("TestCase", "replica upload update initial_content_type expected_content_type")
        with override_bucket_config(BucketConfig.TEST):
            key = f"operations/{uuid.uuid4()}"
            tests = [
                TestCase(Replica.aws, self._put_s3_file, storage.update_aws_content_type, "a", "b"),
                TestCase(Replica.gcp, self._put_gs_file, storage.update_gcp_content_type, "a", "b")
            ]
            data = b"foo"
            for test in tests:
                with self.subTest(test.replica.name):
                    handle = Config.get_blobstore_handle(test.replica)
                    native_handle = Config.get_native_handle(test.replica)
                    test.upload(key, data, test.initial_content_type)
                    test.update(native_handle, test.replica.bucket, key, test.expected_content_type)
                    self.assertEqual(test.expected_content_type, handle.get_content_type(test.replica.bucket, key))
                    self.assertEqual(handle.get(test.replica.bucket, key), data)

    def _put_s3_file(self, key, data, content_type="blah"):
        s3 = Config.get_native_handle(Replica.aws)
        s3.put_object(Bucket=Replica.aws.bucket, Key=key, Body=data, ContentType=content_type)

    def _put_gs_file(self, key, data, content_type="blah"):
        gs = Config.get_native_handle(Replica.gcp)
        gs_bucket = gs.bucket(Replica.gcp.bucket)
        gs_blob = gs_bucket.blob(key, chunk_size=1 * 1024 * 1024)
        with io.BytesIO(data) as fh:
            gs_blob.upload_from_file(fh, content_type="application/octet-stream")

if __name__ == '__main__':
    unittest.main()
