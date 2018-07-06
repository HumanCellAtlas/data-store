#!/usr/bin/env python
# coding: utf-8
import datetime
import json
import os
import sys
import unittest
import uuid
from uuid import UUID

import requests

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from cloud_blobstore import BlobNotFoundError
import dss
from dss.config import override_bucket_config, BucketConfig, Replica, Config
from dss.util import UrlBuilder
from dss.util.version import datetime_to_version_format
from dss.storage.checkout import (
    CheckoutStatus,
    ValidationEnum,
    get_dst_key,
    get_execution_id,
    get_manifest_files,
    pre_exec_validate,
    validate_bundle_exists,
    validate_file_dst,
    touch_test_file,
    start_file_checkout,
)
from dss.storage.hcablobstore import BundleMetadata, compose_blob_key
from dss.storage.identifiers import BundleFQID
from tests import eventually
from tests.infra import DSSAssertMixin, DSSUploadMixin, get_env, testmode
from tests.infra.server import ThreadedLocalServer


class TestCheckoutApi(unittest.TestCase, DSSAssertMixin, DSSUploadMixin):
    test_bundle_uploaded: bool = False
    bundle_uuid: str
    bundle_version: str
    file_uuid: str
    file_version: str

    @classmethod
    def setUpClass(cls):
        cls.app = ThreadedLocalServer()
        cls.app.start()

    @classmethod
    def tearDownClass(cls):
        cls.app.shutdown()

    def setUp(self):
        dss.Config.set_config(dss.BucketConfig.TEST)

        # this should really belong in setUpClass, but this code depends on DSSAssertMixin, which only works on class
        # instances, not classes.
        if not TestCheckoutApi.test_bundle_uploaded:
            TestCheckoutApi.test_bundle_uploaded = True

            TestCheckoutApi.bundle_uuid = str(uuid.uuid4())
            TestCheckoutApi.bundle_version = datetime_to_version_format(datetime.datetime.utcnow())
            TestCheckoutApi.file_uuid = str(uuid.uuid4())
            TestCheckoutApi.file_version = datetime_to_version_format(datetime.datetime.utcnow())
            for replica in Replica:
                fixtures_bucket = self.get_test_fixture_bucket(replica)

                self.upload_file_wait(
                    f"{replica.storage_schema}://{fixtures_bucket}/test_good_source_data/0",
                    replica,
                    TestCheckoutApi.file_uuid,
                    file_version=TestCheckoutApi.file_version,
                    bundle_uuid=TestCheckoutApi.bundle_uuid,
                )

                builder = UrlBuilder().set(path="/v1/bundles/" + TestCheckoutApi.bundle_uuid)
                builder.add_query("replica", replica.name)
                builder.add_query("version", TestCheckoutApi.bundle_version)
                url = str(builder)

                self.assertPutResponse(
                    url,
                    [requests.codes.ok, requests.codes.created],
                    json_request_body=dict(
                        files=[
                            dict(
                                uuid=TestCheckoutApi.file_uuid,
                                version=TestCheckoutApi.file_version,
                                name="blah blah",
                                indexed=False,
                            )
                        ],
                        creator_uid=12345,
                    ),
                )

    def get_test_fixture_bucket(self, replica: Replica) -> str:
        if replica == Replica.aws:
            bucket = get_env("DSS_S3_BUCKET_TEST_FIXTURES")
        elif replica == Replica.gcp:
            bucket = get_env("DSS_GS_BUCKET_TEST_FIXTURES")
        return bucket

    @testmode.integration
    def test_pre_execution_check_doesnt_exist(self):
        for replica in Replica:
            non_existent_bundle_uuid = str(uuid.uuid4())
            non_existent_bundle_version = datetime_to_version_format(datetime.datetime.utcnow())
            request_body = {"destination": replica.checkout_bucket}

            url = str(UrlBuilder()
                      .set(path="/v1/bundles/" + non_existent_bundle_uuid + "/checkout")
                      .add_query("replica", replica.name)
                      .add_query("version", non_existent_bundle_version))

            resp_obj = self.assertPostResponse(
                url,
                requests.codes.not_found,
                request_body
            )
            self.assertEqual(resp_obj.json['code'], 'not_found')

    @testmode.integration
    def test_sanity_check_no_replica(self):
        for replica in Replica:
            request_body = {"destination": replica.checkout_bucket}

            url = str(UrlBuilder()
                      .set(path="/v1/bundles/" + self.bundle_uuid + "/checkout")
                      .add_query("replica", "")
                      .add_query("version", self.bundle_version))

            self.assertPostResponse(
                url,
                requests.codes.bad_request,
                request_body
            )

    def launch_checkout(self, dst_bucket: str, replica: Replica) -> str:
        request_body = {"destination": dst_bucket}

        url = str(UrlBuilder()
                  .set(path="/v1/bundles/" + self.bundle_uuid + "/checkout")
                  .add_query("replica", replica.name)
                  .add_query("version", self.bundle_version))

        resp_obj = self.assertPostResponse(
            url,
            requests.codes.ok,
            request_body
        )
        execution_id = resp_obj.json["checkout_job_id"]
        self.assertIsNotNone(execution_id)

        return execution_id

    @testmode.integration
    def test_status_success(self):
        for replica in Replica:
            execution_id = self.launch_checkout(replica.checkout_bucket, replica)
            CheckoutStatus.mark_bundle_checkout_started(execution_id, replica, replica.checkout_bucket)

            url = str(UrlBuilder().set(path="/v1/bundles/checkout/" + execution_id).add_query("replica", replica.name))

            @eventually(timeout=120, interval=1)
            def check_status():
                resp_obj = self.assertGetResponse(
                    url,
                    requests.codes.ok
                )
                status = resp_obj.json.get('status')
                if status not in ("RUNNING", "SUCCEEDED"):
                    raise Exception(f"Unexpected status {status}")
                self.assertEqual(status, "SUCCEEDED")

            check_status()

    @testmode.integration
    def test_status_fail(self):
        nonexistent_bucket_name = str(uuid.uuid4())
        for replica in Replica:
            execution_id = self.launch_checkout(nonexistent_bucket_name, replica)
            CheckoutStatus.mark_bundle_checkout_started(execution_id, replica, replica.checkout_bucket)

            url = str(UrlBuilder().set(path="/v1/bundles/checkout/" + execution_id).add_query("replica", replica.name))

            @eventually(timeout=120, interval=1)
            def check_status():
                resp_obj = self.assertGetResponse(
                    url,
                    requests.codes.ok
                )
                status = resp_obj.json.get('status')
                if status not in ("RUNNING", "FAILED"):
                    raise Exception(f"Unexpected status {status}")
                self.assertEqual(status, "FAILED")

            check_status()

    @testmode.standalone
    def test_get_unknown_checkout(self):
        nonexistent_checkout_execution_id = str(uuid.uuid4())
        for replica in Replica:
            url = str(UrlBuilder()
                      .set(path="/v1/bundles/checkout/" + nonexistent_checkout_execution_id)
                      .add_query("replica", replica.name))

            resp_obj = self.assertGetResponse(
                url,
                requests.codes.not_found
            )
            self.assertEqual(resp_obj.json['code'], "not_found")

    @testmode.integration
    def test_checkout_file_success(self):
        for replica in Replica:
            bundle_fqid = BundleFQID(uuid=self.bundle_uuid, version=self.bundle_version)

            handle = Config.get_blobstore_handle(replica)
            # retrieve the bundle metadata.
            bundle_metadata = json.loads(
                handle.get(
                    replica.bucket,
                    bundle_fqid.to_key(),
                ).decode("utf-8"))
            blob_key = compose_blob_key(bundle_metadata[BundleMetadata.FILES][0])
            start_file_checkout(replica, blob_key)

            @eventually(timeout=120, interval=1)
            def check_status():
                self.assertTrue(validate_file_dst(replica, replica.checkout_bucket, get_dst_key(blob_key)))

            check_status()

    @testmode.standalone
    def test_manifest_files(self):
        bundle_uuid = "011c7340-9b3c-4d62-bf49-090d79daf198"
        version = "2017-06-20T214506.766634Z"
        replica = Replica.aws
        file_count = 0
        with override_bucket_config(BucketConfig.TEST_FIXTURE):
            for _ in get_manifest_files(replica, replica.bucket, bundle_uuid, version):
                file_count += 1
        self.assertEqual(file_count, 1)

    @testmode.standalone
    def test_validate_file_dst_fail(self):
        nonexistent_dst_key = str(uuid.uuid4())
        for replica in Replica:
            self.assertEquals(validate_file_dst(replica, replica.checkout_bucket, nonexistent_dst_key), False)

    @testmode.standalone
    def test_validate_file_dst(self):
        dst_key = "files/ce55fd51-7833-469b-be0b-5da88ebebfcd.2017-06-18T075702.020366Z"
        for replica in Replica:
            bucket = self.get_test_fixture_bucket(replica)
            self.assertEquals(validate_file_dst(replica, bucket, dst_key), True)

    @testmode.standalone
    def test_validate_wrong_key(self):
        nonexistent_bundle_uuid = str(uuid.uuid4())
        nonexistent_bundle_version = datetime_to_version_format(datetime.datetime.utcnow())
        for replica in Replica:
            valid, cause = pre_exec_validate(replica, replica.bucket, replica.checkout_bucket, nonexistent_bundle_uuid,
                                             nonexistent_bundle_version)
            self.assertIs(valid, ValidationEnum.WRONG_BUNDLE_KEY, msg=f"cause: {cause}")

    @testmode.standalone
    def test_validate(self):
        for replica in Replica:
            valid, cause = pre_exec_validate(replica, replica.bucket, replica.checkout_bucket, self.bundle_uuid,
                                             self.bundle_version)
            self.assertIs(valid, ValidationEnum.PASSED)

    @testmode.standalone
    def test_validate_bundle_exists(self):
        for replica in Replica:
            valid, cause = validate_bundle_exists(replica, replica.bucket, self.bundle_uuid, self.bundle_version)
            self.assertIs(valid, ValidationEnum.PASSED)

    @testmode.standalone
    def test_validate_bundle_exists_fail(self):
        nonexistent_bundle_uuid = str(uuid.uuid4())
        nonexistent_bundle_version = datetime_to_version_format(datetime.datetime.utcnow())
        for replica in Replica:
            valid, cause = validate_bundle_exists(replica, self.get_test_fixture_bucket(replica),
                                                  nonexistent_bundle_uuid, nonexistent_bundle_version)
            self.assertIs(valid, ValidationEnum.WRONG_BUNDLE_KEY)

    @testmode.standalone
    def test_touch_file(self):
        for replica in Replica:
            self.assertEqual(touch_test_file(replica, replica.checkout_bucket), True)

    @testmode.standalone
    def test_execution_id(self):
        exec_id = get_execution_id()
        self.assertIsNotNone(exec_id)
        try:
            UUID(exec_id, version=4)
        except ValueError:
            self.fail("Invalid execution id. Valid UUID v.4 is expected.")

    @testmode.standalone
    def test_status_succeeded(self):
        fake_bucket_name = str(uuid.uuid4())
        fake_location = str(uuid.uuid4())
        exec_id = get_execution_id()
        self.assertIsNotNone(exec_id)
        for replica in Replica:
            CheckoutStatus.mark_bundle_checkout_successful(
                exec_id, replica, replica.checkout_bucket, fake_bucket_name, fake_location)
            status = CheckoutStatus.get_bundle_checkout_status(exec_id, replica, replica.checkout_bucket)
            self.assertEquals(status['status'], "SUCCEEDED")
            self.assertEquals(status['location'], f"{replica.storage_schema}://{fake_bucket_name}/{fake_location}")

    @testmode.standalone
    def test_status_failed(self):
        exec_id = get_execution_id()
        self.assertIsNotNone(exec_id)
        cause = 'Fake cause'
        for replica in Replica:
            CheckoutStatus.mark_bundle_checkout_failed(exec_id, replica, replica.checkout_bucket, cause)
            status = CheckoutStatus.get_bundle_checkout_status(exec_id, replica, replica.checkout_bucket)
            self.assertEquals(status['status'], "FAILED")
            self.assertEquals(status['cause'], cause)

    @testmode.standalone
    def test_status_started(self):
        exec_id = get_execution_id()
        self.assertIsNotNone(exec_id)
        for replica in Replica:
            CheckoutStatus.mark_bundle_checkout_started(exec_id, replica, replica.checkout_bucket)
            status = CheckoutStatus.get_bundle_checkout_status(exec_id, replica, replica.checkout_bucket)
            self.assertEquals(status['status'], "RUNNING")

    @testmode.standalone
    def test_status_wrong_jobid(self):
        exec_id = get_execution_id()
        self.assertIsNotNone(exec_id)
        for replica in Replica:
            with self.assertRaises(BlobNotFoundError):
                CheckoutStatus.get_bundle_checkout_status(exec_id, replica, replica.checkout_bucket)


if __name__ == "__main__":
    unittest.main()
