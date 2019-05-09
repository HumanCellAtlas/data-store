#!/usr/bin/env python
# coding: utf-8

import io
import os
import sys
import uuid
import unittest
from collections import namedtuple
from unittest import mock

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from tests.infra import testmode
from dss.operations import DSSOperationsCommandDispatch
from dss.operations.util import map_bucket_results, CommandForwarder
from dss.operations.storage import update_aws_content_type, update_gcp_content_type
from dss.logging import configure_test_logging
from dss.config import BucketConfig, Config, Replica, override_bucket_config

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

    def test_update_content_type(self):
        TestCase = namedtuple("TestCase", "replica upload update initial_content_type expected_content_type")
        with override_bucket_config(BucketConfig.TEST):
            key = f"operations/{uuid.uuid4()}"
            tests = [
                TestCase(Replica.aws, self._put_s3_file, update_aws_content_type, "a", "b"),
                TestCase(Replica.gcp, self._put_gs_file, update_gcp_content_type, "a", "b")
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
