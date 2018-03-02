#!/usr/bin/env python
# coding: utf-8

import os
from uuid import UUID
import requests
import sys
import unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss.config import override_bucket_config, BucketConfig, Replica
from dss.util import UrlBuilder
from dss.storage.checkout import get_manifest_files, validate_file_dst, pre_exec_validate, ValidationEnum, \
    validate_bundle_exists, touch_test_file, get_execution_id
from tests.infra import DSSAssertMixin, DSSUploadMixin, get_env, testmode
from tests.infra.server import ThreadedLocalServer


class TestFileApi(unittest.TestCase, DSSAssertMixin, DSSUploadMixin):
    @classmethod
    def setUpClass(cls):
        cls.app = ThreadedLocalServer()
        cls.app.start()

    @classmethod
    def tearDownClass(cls):
        cls.app.shutdown()

    def setUp(self):
        dss.Config.set_config(dss.BucketConfig.TEST)

    def get_test_fixture_bucket(self, replica: Replica) -> str:
        if replica == Replica.aws:
            bucket = get_env("DSS_S3_BUCKET_TEST_FIXTURES")
        elif replica == Replica.gcp:
            bucket = get_env("DSS_GS_BUCKET_TEST_FIXTURES")
        return bucket

    @testmode.integration
    def test_pre_execution_check_doesnt_exist(self):
        for replica in Replica:
            non_existent_bundle_uuid = "011c7340-9b3c-4d62-bf49-090d79daf111"
            version = "2017-06-20T214506.766634Z"
            request_body = {"destination": replica.checkout_bucket, "email": "rkisin@chanzuckerberg.com"}

            url = str(UrlBuilder()
                      .set(path="/v1/bundles/" + non_existent_bundle_uuid + "/checkout")
                      .add_query("replica", replica.name)
                      .add_query("version", version))

            with override_bucket_config(BucketConfig.TEST_FIXTURE):
                resp_obj = self.assertPostResponse(
                    url,
                    requests.codes.not_found,
                    request_body
                )
            self.assertEqual(resp_obj.json['code'], 'not_found')

    @testmode.integration
    def test_sanity_check_no_replica(self):
        bundle_uuid = "011c7340-9b3c-4d62-bf49-090d79daf198"
        version = "2017-06-20T214506.766634Z"
        for replica in Replica:
            request_body = {"destination": replica.checkout_bucket, "email": "rkisin@chanzuckerberg.com"}

            url = str(UrlBuilder()
                      .set(path="/v1/bundles/" + bundle_uuid + "/checkout")
                      .add_query("replica", "")
                      .add_query("version", version))

            with override_bucket_config(BucketConfig.TEST_FIXTURE):
                self.assertPostResponse(
                    url,
                    requests.codes.bad_request,
                    request_body
                )

    def launch_checkout(self, dst_bucket: str, replica: Replica) -> str:
        bundle_uuid = "011c7340-9b3c-4d62-bf49-090d79daf198"
        version = "2017-06-20T214506.766634Z"
        request_body = {"destination": dst_bucket, "email": "rkisin@chanzuckerberg.com"}

        url = str(UrlBuilder()
                  .set(path="/v1/bundles/" + bundle_uuid + "/checkout")
                  .add_query("replica", replica.name)
                  .add_query("version", version))

        with override_bucket_config(BucketConfig.TEST_FIXTURE):
            resp_obj = self.assertPostResponse(
                url,
                requests.codes.ok,
                request_body
            )
        execution_arn = resp_obj.json["checkout_job_id"]
        self.assertIsNotNone(execution_arn)

        return execution_arn

    @testmode.integration
    def test_status_success(self):
        for replica in Replica:
            exec_arn = self.launch_checkout(replica.checkout_bucket, replica)
            url = str(UrlBuilder().set(path="/v1/bundles/checkout/" + exec_arn))
            with override_bucket_config(BucketConfig.TEST_FIXTURE):
                resp_obj = self.assertGetResponse(
                    url,
                    requests.codes.ok
                )
            status = resp_obj.json.get('status')
            self.assertIsNotNone(status)
            self.assertIn(status, ['RUNNING', 'SUCCEEDED'])

    @testmode.integration
    def test_status_fail(self):
        for replica in Replica:
            exec_arn = self.launch_checkout('e47114c9-bb96-480f-b6f5-c3e07aae399f', replica)
            url = str(UrlBuilder().set(path="/v1/bundles/checkout/" + exec_arn))
            with override_bucket_config(BucketConfig.TEST_FIXTURE):
                resp_obj = self.assertGetResponse(
                    url,
                    requests.codes.ok
                )
            status = resp_obj.json.get('status')
            self.assertIsNotNone(status)
            self.assertIn(status, ['RUNNING', 'FAILED'])

    @testmode.standalone
    def test_manifest_files(self):
        bundle_uuid = "011c7340-9b3c-4d62-bf49-090d79daf198"
        version = "2017-06-20T214506.766634Z"
        replica = Replica.aws
        file_count = 0
        with override_bucket_config(BucketConfig.TEST_FIXTURE):
            for _ in get_manifest_files(bundle_uuid, version, replica):
                file_count += 1
        self.assertEqual(file_count, 1)

    @testmode.standalone
    def test_validate_file_dst_fail(self):
        dst_key = "83b76ac9-2470-46d2-ae5e-a415ce86b020"
        for replica in Replica:
            self.assertEquals(validate_file_dst(replica.checkout_bucket, dst_key, replica), False)

    @testmode.standalone
    def test_validate_file_dst(self):
        dst_key = "files/ce55fd51-7833-469b-be0b-5da88ebebfcd.2017-06-18T075702.020366Z"
        for replica in Replica:
            bucket = self.get_test_fixture_bucket(replica)
            self.assertEquals(validate_file_dst(bucket, dst_key, replica), True)

    @testmode.standalone
    def test_validate_wrong_key(self):
        bundle_uuid = "WRONG_KEY"
        version = "WRONG_VERSION"
        for replica in Replica:
            valid, cause = pre_exec_validate(replica.bucket, replica.checkout_bucket, replica, bundle_uuid, version)
            self.assertIs(valid, ValidationEnum.WRONG_BUNDLE_KEY)

    @testmode.standalone
    def test_validate(self):
        bundle_uuid = "011c7340-9b3c-4d62-bf49-090d79daf198"
        version = "2017-06-20T214506.766634Z"
        for replica in Replica:
            valid, cause = pre_exec_validate(self.get_test_fixture_bucket(replica), replica.checkout_bucket, replica,
                                             bundle_uuid, version)
            self.assertIs(valid, ValidationEnum.PASSED)

    @testmode.standalone
    def test_validate_bundle_exists(self):
        bundle_uuid = "011c7340-9b3c-4d62-bf49-090d79daf198"
        version = "2017-06-20T214506.766634Z"
        for replica in Replica:
            valid, cause = validate_bundle_exists(replica, self.get_test_fixture_bucket(replica), bundle_uuid, version)
            self.assertIs(valid, ValidationEnum.PASSED)

    @testmode.standalone
    def test_validate_bundle_exists_fail(self):
        bundle_uuid = "WRONG_KEY"
        version = "WRONG_VERSION"
        for replica in Replica:
            valid, cause = validate_bundle_exists(replica, self.get_test_fixture_bucket(replica), bundle_uuid, version)
            self.assertIs(valid, ValidationEnum.WRONG_BUNDLE_KEY)

    @testmode.standalone
    def test_touch_file(self):
        for replica in Replica:
            self.assertEqual(touch_test_file(replica.checkout_bucket, replica), True)

    @testmode.standalone
    def test_execution_id(self):
        exec_id = get_execution_id()
        self.assertIsNotNone(exec_id)
        try:
            UUID(exec_id, version=4)
        except ValueError:
            self.fail("Invalid execution id. Valid UUID v.4 is expected.")

if __name__ == "__main__":
    unittest.main()
