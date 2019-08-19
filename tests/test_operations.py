#!/usr/bin/env python
# coding: utf-8

import io
import os
import sys
import uuid
import json
import argparse
import unittest
import string
import random
from collections import namedtuple
from unittest import mock
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

from cloud_blobstore import BlobNotFoundError

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from tests.infra import testmode
from dss.operations import DSSOperationsCommandDispatch
from dss.operations.util import map_bucket_results
from dss.operations import storage, sync, secrets, params
from dss.logging import configure_test_logging
from dss.config import BucketConfig, Config, Replica, override_bucket_config
from dss.storage.hcablobstore import FileMetadata, compose_blob_key
from dss.util.aws import resources
from tests import CaptureStdout

def setUpModule():
    configure_test_logging()

def random_alphanumeric_string(N=10):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=N))

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
                    storage.repair_file_blob_metadata([], args).process_key(key)
                    self.assertEqual(handle.get_content_type(replica.bucket, blob_key),
                                     file_metadata[FileMetadata.CONTENT_TYPE])

                with self.subTest("Should handle arbitrary exceptions", replica=replica):
                    with mock.patch("dss.operations.storage.StorageOperationHandler.log_error") as log_error:
                        with mock.patch("dss.config.Config.get_native_handle") as thrower:
                            thrower.side_effect = Exception()
                            storage.repair_file_blob_metadata([], args).process_key(key)
                            log_error.assert_called()
                            self.assertEqual(log_error.call_args[0][0], "Exception")

                with self.subTest("Should handle missing file metadata", replica=replica):
                    with mock.patch("dss.operations.storage.StorageOperationHandler.log_warning") as log_warning:
                        storage.repair_file_blob_metadata([], args).process_key("wrong key")
                        self.assertEqual(log_warning.call_args[0][0], "BlobNotFoundError")

                with self.subTest("Should handle missing blob", replica=replica):
                    with mock.patch("dss.operations.storage.StorageOperationHandler.log_warning") as log_warning:
                        file_metadata[FileMetadata.SHA256] = "wrong"
                        uploader[replica](key, json.dumps(file_metadata).encode("utf-8"), "application/json")
                        storage.repair_file_blob_metadata([], args).process_key(key)
                        self.assertEqual(log_warning.call_args[0][0], "BlobNotFoundError")

                with self.subTest("Should handle corrupt file metadata", replica=replica):
                    with mock.patch("dss.operations.storage.StorageOperationHandler.log_warning") as log_warning:
                        uploader[replica](key, b"this is not json", "application/json")
                        storage.repair_file_blob_metadata([], args).process_key(key)
                        self.assertEqual(log_warning.call_args[0][0], "JSONDecodeError")

    def test_update_content_type(self):
        TestCase = namedtuple("TestCase", "replica upload size update initial_content_type expected_content_type")
        with override_bucket_config(BucketConfig.TEST):
            key = f"operations/{uuid.uuid4()}"
            large_size = 64 * 1024 * 1024 + 1
            tests = [
                TestCase(Replica.aws, self._put_s3_file, 1, storage.update_aws_content_type, "a", "b"),
                TestCase(Replica.aws, self._put_s3_file, large_size, storage.update_aws_content_type, "a", "b"),
                TestCase(Replica.gcp, self._put_gs_file, 1, storage.update_gcp_content_type, "a", "b"),
            ]
            for test in tests:
                data = os.urandom(test.size)
                with self.subTest(test.replica.name):
                    handle = Config.get_blobstore_handle(test.replica)
                    native_handle = Config.get_native_handle(test.replica)
                    test.upload(key, data, test.initial_content_type)
                    old_checksum = handle.get_cloud_checksum(test.replica.bucket, key)
                    test.update(native_handle, test.replica.bucket, key, test.expected_content_type)
                    self.assertEqual(test.expected_content_type, handle.get_content_type(test.replica.bucket, key))
                    self.assertEqual(handle.get(test.replica.bucket, key), data)
                    self.assertEqual(old_checksum, handle.get_cloud_checksum(test.replica.bucket, key))

    def test_verify_blob_replication(self):
        key = "blobs/alsdjflaskjdf"
        from_handle = mock.Mock()
        to_handle = mock.Mock()
        from_handle.get_size = mock.Mock(return_value=10)
        to_handle.get_size = mock.Mock(return_value=10)

        with self.subTest("no replication error"):
            res = sync.verify_blob_replication(from_handle, to_handle, "", "", key)
            self.assertEqual(res, list())

        with self.subTest("Unequal size blobs reports error"):
            to_handle.get_size = mock.Mock(return_value=11)
            res = sync.verify_blob_replication(from_handle, to_handle, "", "", key)
            self.assertEqual(res[0].key, key)
            self.assertIn("mismatch", res[0].anomaly)

        with self.subTest("Missing target blob reports error"):
            to_handle.get_size.side_effect = BlobNotFoundError
            res = sync.verify_blob_replication(from_handle, to_handle, "", "", key)
            self.assertEqual(res[0].key, key)
            self.assertIn("missing", res[0].anomaly)

    def test_verify_file_replication(self):
        key = "blobs/alsdjflaskjdf"
        from_handle = mock.Mock()
        to_handle = mock.Mock()
        file_metadata = json.dumps({'sha256': "", 'sha1': "", 's3-etag': "", 'crc32c': ""})
        from_handle.get = mock.Mock(return_value=file_metadata)
        to_handle.get = mock.Mock(return_value=file_metadata)

        with self.subTest("no replication error"):
            with mock.patch("dss.operations.sync.verify_blob_replication") as vbr:
                vbr.return_value = list()
                res = sync.verify_file_replication(from_handle, to_handle, "", "", key)
                self.assertEqual(res, list())

        with self.subTest("Unequal file metadata"):
            to_handle.get.return_value = "{}"
            res = sync.verify_file_replication(from_handle, to_handle, "", "", key)
            self.assertEqual(res[0].key, key)
            self.assertIn("mismatch", res[0].anomaly)

        with self.subTest("Missing file metadata"):
            to_handle.get.side_effect = BlobNotFoundError
            res = sync.verify_file_replication(from_handle, to_handle, "", "", key)
            self.assertEqual(res[0].key, key)
            self.assertIn("missing", res[0].anomaly)

    def test_verify_bundle_replication(self):
        key = "blobs/alsdjflaskjdf"
        from_handle = mock.Mock()
        to_handle = mock.Mock()
        bundle_metadata = json.dumps({
            "creator_uid": 8008,
            "files": [{"uuid": None, "version": None}]
        })
        from_handle.get = mock.Mock(return_value=bundle_metadata)
        to_handle.get = mock.Mock(return_value=bundle_metadata)

        with mock.patch("dss.operations.sync.verify_file_replication") as vfr:
            with self.subTest("replication ok"):
                vfr.return_value = list()
                res = sync.verify_bundle_replication(from_handle, to_handle, "", "", key)
                self.assertEqual(res, [])

            with self.subTest("replication problem"):
                vfr.return_value = [sync.ReplicationAnomaly(key="", anomaly="")]
                res = sync.verify_bundle_replication(from_handle, to_handle, "", "", key)
                self.assertEqual(res, vfr.return_value)

            with self.subTest("Unequal bundle metadata"):
                to_handle.get.return_value = "{}"
                res = sync.verify_bundle_replication(from_handle, to_handle, "", "", key)
                self.assertEqual(res[0].key, key)
                self.assertIn("mismatch", res[0].anomaly)

            with self.subTest("Missing destination bundle metadata"):
                to_handle.get.side_effect = BlobNotFoundError
                res = sync.verify_bundle_replication(from_handle, to_handle, "", "", key)
                self.assertEqual(res[0].key, key)
                self.assertIn("missing on target", res[0].anomaly)

            with self.subTest("Missing source bundle metadata"):
                from_handle.get.side_effect = BlobNotFoundError
                res = sync.verify_bundle_replication(from_handle, to_handle, "", "", key)
                self.assertEqual(res[0].key, key)
                self.assertIn("missing on source", res[0].anomaly)

    def _put_s3_file(self, key, data, content_type="blah", part_size=None):
        s3 = Config.get_native_handle(Replica.aws)
        with io.BytesIO(data) as fh:
            s3.upload_fileobj(Bucket=Replica.aws.bucket,
                              Key=key,
                              Fileobj=fh,
                              ExtraArgs=dict(ContentType=content_type),
                              Config=TransferConfig(multipart_chunksize=64 * 1024 * 1024))

    def _put_gs_file(self, key, data, content_type="blah"):
        gs = Config.get_native_handle(Replica.gcp)
        gs_bucket = gs.bucket(Replica.gcp.bucket)
        gs_blob = gs_bucket.blob(key, chunk_size=1 * 1024 * 1024)
        with io.BytesIO(data) as fh:
            gs_blob.upload_from_file(fh, content_type="application/octet-stream")

    def test_secrets_crud(self):
        # CRUD (create read update delete)
        # test for the secrets manager.
        # 
        # Procedure:
        # - create new secret
        # - list secrets and verify new secret shows up
        # - get secret value and verify it is correct
        # - update secret value
        # - get secret value and verify it is correct
        # - delete secret
        which_stage = os.environ["DSS_DEPLOYMENT_STAGE"]
        which_store = os.environ["DSS_SECRETS_STORE"]

        secret_name = random_alphanumeric_string()
        testvar_name = f"{which_store}/{which_stage}/{secret_name}"
        testvar_value = "Hello world!"
        testvar_value2 = "Goodbye world!"

        unusedvar_name = f"{which_store}/{which_stage}/admin_user_emails"

        with self.subTest("Create a new secret"):
            # Monkeypatch the secrets manager
            with mock.patch("dss.operations.secrets.secretsmanager") as sm:
                # Creating a new variable will first call get, which will not find it
                sm.get_secret_value = mock.MagicMock(return_value=None, side_effect=ClientError({}, None))
                # Next we will use the create secret command
                sm.create_secret = mock.MagicMock(return_value=None)
                # Create initial secret value
                secrets.set_secret(
                    [],
                    argparse.Namespace(
                        secret_name=testvar_name,
                        secret_value=testvar_value,
                        dry_run=False
                    ),
                )

        with self.subTest("List secrets"):
            with mock.patch("dss.operations.secrets.secretsmanager") as sm:
                # Listing secrets requires creating a paginator first,
                # so mock what the paginator returns
                class MockPaginator(object):
                    def paginate(self):
                        # Return a mock page from the mock paginator
                        return [
                            {
                                "SecretList": [
                                    {
                                        "Name": testvar_name
                                    },
                                    {
                                        "Name": unusedvar_name
                                    }
                                ]
                            }
                        ]
                sm.get_paginator.return_value = MockPaginator()
                # Test variable name should be in list of secret names
                with CaptureStdout() as output:
                    secrets.list_secrets([], argparse.Namespace(json=False))
                self.assertIn(testvar_name, output)

        with self.subTest("Get secret value"):
            with mock.patch("dss.operations.secrets.secretsmanager") as sm:
                # Requesting the variable will try to get secret value and succeed
                sm.get_secret_value.return_value = {"SecretString": testvar_value}
                # Now run get secret value in JSON mode and non-JSON mode
                # and verify variable name/value is in both.
                #
                # Start with non-JSON get call:
                # Single variable:
                with CaptureStdout() as output:
                    secrets.get_secret(
                        [], argparse.Namespace(secret_names=[testvar_name], json=False)
                    )
                output = "".join(output)
                self.assertIn(testvar_name, output)
                self.assertIn(testvar_value, output)
                # Multiple variables:
                with CaptureStdout() as output:
                    secrets.get_secret(
                        [],
                        argparse.Namespace(
                            secret_names=[testvar_name, unusedvar_name],
                            json=False
                        )
                    )
                output = "".join(output)
                self.assertIn(testvar_name, output)
                self.assertIn(testvar_value, output)
                #
                # Now JSON get call:
                # Single variable:
                with CaptureStdout() as output:
                    secrets.get_secret(
                        [],
                        argparse.Namespace(secret_names=[testvar_name], json=True),
                    )
                output = "".join(output)
                self.assertIn(testvar_name, output)
                self.assertIn(testvar_value, output)
                # Multiple variables:
                with CaptureStdout() as output:
                    secrets.get_secret(
                        [],
                        argparse.Namespace(
                            secret_names=[testvar_name, unusedvar_name],
                            json=True
                        )
                    )
                output = "".join(output)
                self.assertIn(testvar_name, output)
                self.assertIn(testvar_value, output)

        with self.subTest("Update existing secret"):
            with mock.patch("dss.operations.secrets.secretsmanager") as sm:
                # Updating the variable will try to get secret value and succeed
                sm.get_secret_value = mock.MagicMock(return_value={"SecretString": testvar_value})
                # Next we will call the update secret command
                sm.update_secret = mock.MagicMock(return_value=None)
                # Update secret
                secrets.set_secret(
                    [],
                    argparse.Namespace(
                        secret_name=testvar_name,
                        secret_value=testvar_value2,
                        dry_run=False,
                    ),
                )

        with self.subTest("Delete secret"):
            with mock.patch("dss.operations.secrets.secretsmanager") as sm:
                # Deleting the variable will try to get secret value and succeed
                sm.get_secret_value = mock.MagicMock(return_value={"SecretString": testvar_value})
                sm.delete_secret = mock.MagicMock(return_value=None)
                # Delete secret
                secrets.del_secret(
                    [],
                    argparse.Namespace(
                        secret_name=testvar_name,
                        force=True,
                        dry_run=False,
                    ),
                )


    def test_ssmparams_crud(self):
        # CRUD (create read update delete)
        # test for setting environment variables
        # in the SSM store.
        #
        # Procedure:
        # - create new parameter in ssm store
        # - list ssm parameters, verify new param is set
        # - update param value
        # - list ssm parameters, verify new param is set
        # - delete param
        which_stage = os.environ["DSS_DEPLOYMENT_STAGE"]
        which_store = os.environ["DSS_PARAMS_STORE"]

        param_name = random_alphanumeric_string()
        testvar_name = f"{which_store}/{which_stage}/{param_name}"
        testvar_value = "Hello world!"
        testvar_value2 = "Goodbye world!"

        # Package up the environment the way
        # AWS returns it
        def wrap_environment(e):
            ssm_e = {"Parameter" : {
                "Name": "environment",
                "Value": json.dumps(e)
            }
            return ssm_e

        # Assemble an old and new environment to return
        old_env = {"dummy_key": "dummy_value"}
        new_env = dict(**old_env)
        new_env[testvar_name] = testvar_value)
        ssm_old_env = wrap_env(old_env)
        ssm_new_env = wrap_env(new_env)

        with self.subTest("Create a new SSM parameter"):
            # Monkeypatch ssm client
            with mock.patch("dss.operations.ssm") as ssm:
                # ssm_set in params.py first calls ssm.get_parameter
                # to get the entire environment
                ssm.get_parameter = mock.MagicMock(return_value=ssm_old_env)
                # ssm_set then calls ssm.put_parameter
                ssm.put_parameter = mock.MagicMock(return_value=None)
                params.ssm_set(
                    [],
                    argparse.Namespace(
                        name=testvar_name,
                        value=testvar_value,
                        dry_run=False
                    ),
                )

        with self.subTest("List SSM parameters"):
            # TODO: test json too
            with mock.patch("dss.operations.ssm") as ssm:
                # Listing parameters from the environment
                # does not require a pager, it only calls
                # ssm.get_parameter, which asks for the 
                # entire environment as a dictionary
                ssm.get_parameter = mock.MagicMock(return_value=ssm_new_env)
                # Now call our params.py module
                with CaptureStdout() as output:
                    params.ssm_list([], argparse.Namespace(json=False))
                assertIn("{testvar_name}={testvar_value}", output)


        with self.subTest("Update existing SSM parameter"):
            with mock.patch("dss.operations.ssm") as ssm:
                # Mock the same way we did for set new secret above
                ssm.get_parameter = mock.MagicMock(return_value=ssm_new_env)
                ssm.put_parameter = mock.MagicMock(return_value=None)
                params.ssm_set(
                    [],
                    argparse.Namespace(
                        name=testvar_name,
                        value=testvar_value2,
                        dry_run=False
                    ),
                )

        with self.subTest("Unset SSM parameter"):
            with mock.patch("dss.operations.ssm") as ssm:
                # Mock the same way we did for set new secret above
                ssm.get_parameter = mock.MagicMock(return_value=ssm_new_env)
                ssm.put_parameter = mock.MagicMock(return_value=None)
                params.ssm_unset(
                    [],
                    argparse.Namespace(
                        name=testvar_name,
                        dry_run=False
                    )
                )


    def test_lambdaparams_crud(self):
        # CRUD (create read update delete)
        # test for stting environment variables
        # in all deployed lambda functions
        # (and the SSM store).
        #
        # Procedure:
        # - create new parameter for lambdas
        # - list lambda parameters, verify new lambda param is set
        # - get param value and verify correct
        # - update param value
        # - get param value and verify correct
        # - delete param
        which_stage = os.environ["DSS_DEPLOYMENT_STAGE"]
        which_store = os.environ["DSS_PARAMS_STORE"]
        pass

if __name__ == '__main__':
    unittest.main()
