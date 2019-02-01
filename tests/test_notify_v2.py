#!/usr/bin/env python
# coding: utf-8

import os
import io
import sys
import json
from uuid import uuid4
import time
import pytz
import boto3
import botocore.exceptions
import requests
from requests_http_signature import HTTPSignatureAuth
import unittest
import datetime
import typing

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss.util import UrlBuilder
from dss import subscriptions_v2
from dss.events.handlers import notify_v2
from tests.infra import DSSAssertMixin, DSSUploadMixin, get_env, testmode
from dss.config import Replica, BucketConfig, override_bucket_config
from tests.infra.server import ThreadedLocalServer, SilentHandler
from tests import eventually, get_auth_header
from dss.events.handlers.notify_v2 import queue_notification
from dss.util.version import datetime_to_version_format


class MyHandlerClass(SilentHandler):
    """
    Modify ThreadedLocalServer to respond to our notification deliveries.
    """
    hmac_secret_key = "ribos0me"
    def my_handle(self, method):
        if "notification_test_pass_with_auth" in self.path:
            code = 200
            HTTPSignatureAuth.verify(requests.Request(method, self.path, self.headers),
                                     key_resolver=lambda key_id, algorithm: self.hmac_secret_key.encode())
        if "notification_test_pass" in self.path:
            code = 200
        elif "notification_test_fail" in self.path:
            code = 400
        else:
            return self._generic_handle()
        self.send_response(code)
        self.send_header("Content-Length", 0)
        self.end_headers()

    def do_PUT(self):
        self.my_handle("PUT")

    def do_POST(self):
        self.my_handle("POST")


class TestNotifyV2(unittest.TestCase, DSSAssertMixin, DSSUploadMixin):
    @classmethod
    def setUpClass(cls):
        with open(get_env('GOOGLE_APPLICATION_CREDENTIALS'), "r") as fh:
            cls.owner = json.loads(fh.read())['client_email']
        cls.app = ThreadedLocalServer(handler_cls=MyHandlerClass)
        cls.app.start()
        cls.subscription = {
            'owner': cls.owner,
            'uuid': str(uuid4()),
            'callback_url': f"http://127.0.0.1:{cls.app._port}/notification_test_pass",
            'method': "POST",
            'encoding': "application/json",
            'form_fields': {'foo': "bar"},
            'payload_form_field': "baz",
            'replica': "aws",
            'jmespath_query': 'files."assay.json"[?rna.primer==`random`]',
        }
        # TODO: Upload an object, not re-use existing
        cls.bundle_key = "bundles/33327857-c214-40f6-874e-1e197af41540.2018-11-12T235854.981860Z"
        cls.s3 = boto3.client('s3')

    @classmethod
    def tearDownClass(cls):
        for replica in Replica:
            subs = [s for s in subscriptions_v2.get_subscriptions_for_owner(replica, cls.owner)
                    if s['owner'] == cls.owner]
            for s in subs:
                subscriptions_v2.delete_subscription(replica, cls.owner, s['uuid'])

    @testmode.integration
    def test_versioned_tombstone_notifications(self, replica=Replica.aws):
        bucket = get_env('DSS_S3_BUCKET_TEST')
        notification_object_key = f"notification-v2/{uuid4()}"
        url = self.s3.generate_presigned_url(
            ClientMethod='put_object',
            Params=dict(Bucket=bucket, Key=notification_object_key, ContentType="application/json")
        )
        subscription = self._put_subscription(
            {
                'callback_url': url,
                'method': "PUT",
                'jmespath_query': "admin_deleted==`true`"
            },
            replica
        )
        bundle_uuid, bundle_version = self._upload_bundle(replica)
        self._tombstone_bundle(replica, bundle_uuid, bundle_version)

        notification = self._get_notification_from_s3_object(bucket, notification_object_key)
        self.assertEquals(notification['subscription_id'], subscription['uuid'])
        self.assertEquals(notification['match']['bundle_uuid'], bundle_uuid)
        self.assertEquals(notification['match']['bundle_version'], f"{bundle_version}.dead")

    @testmode.integration
    def test_unversioned_tombstone_notifications(self, replica=Replica.aws):
        bucket = get_env('DSS_S3_BUCKET_TEST')
        notification_object_key = f"notification-v2/{uuid4()}"
        url = self.s3.generate_presigned_url(
            ClientMethod='put_object',
            Params=dict(Bucket=bucket, Key=notification_object_key, ContentType="application/json")
        )
        subscription = self._put_subscription(
            {
                'callback_url': url,
                'method': "PUT",
                'jmespath_query': "admin_deleted==`true`"
            },
            replica
        )
        bundle_uuid, bundle_version = self._upload_bundle(replica)
        bundle_uuid, bundle_version = self._upload_bundle(replica, bundle_uuid)
        self._tombstone_bundle(replica, bundle_uuid)

        notification = self._get_notification_from_s3_object(bucket, notification_object_key)
        self.assertEquals(notification['subscription_id'], subscription['uuid'])
        self.assertEquals(notification['match']['bundle_uuid'], bundle_uuid)
        # TODO:
        # Multiple notifications should be delivered for this test. However, the notification
        # test infrastructure (presigned s3 url) cannot track multiple deliveries. Need another
        # mechanism.
        # Brian Hannafious 2019-01-30

    @testmode.integration
    def test_queue_notification(self):
        replica = Replica.aws
        bucket = get_env('DSS_S3_BUCKET_TEST')
        key = f"notification-v2/{uuid4()}"
        post = self.s3.generate_presigned_post(
            Bucket=bucket,
            Key=key,
            ExpiresIn=60,
            Fields={'Content-Type': "application/json"},
            Conditions=[{'Content-Type': "application/json"}]
        )
        subscription = self._put_subscription(
            {
                'payload_form_field': "file",
                'form_fields': post['fields'],
                'callback_url': post['url'],
                'encoding': "multipart/form-data",
            },
            replica
        )

        queue_notification(
            replica,
            subscription,
            "CREATE",
            "bundles/a47b90b2-0967-4fbf-87bc-c6c12db3fedf.2017-07-12T055120.037644Z",
            delay_seconds=0
        )
        notification = self._get_notification_from_s3_object(bucket, key)
        self.assertEquals(notification['subscription_id'], subscription['uuid'])

    @testmode.integration
    def test_bundle_notification(self):
        for replica in Replica:
            with self.subTest(replica):
                self._test_bundle_notification(replica)

    def _test_bundle_notification(self, replica):
        bucket = get_env('DSS_S3_BUCKET_TEST')
        key = f"notification-v2/{uuid4()}"
        url = self.s3.generate_presigned_url(
            ClientMethod='put_object',
            Params=dict(Bucket=bucket, Key=key, ContentType="application/json")
        )
        subscription = self._put_subscription(
            {
                'callback_url': url,
                'method': "PUT",
            },
            replica
        )

        # upload test bundle from test fixtures bucket
        bundle_uuid, bundle_version = self._upload_bundle(replica)

        notification = self._get_notification_from_s3_object(bucket, key)
        self.assertEquals(notification['subscription_id'], subscription['uuid'])
        self.assertEquals(notification['match']['bundle_uuid'], bundle_uuid)
        self.assertEquals(notification['match']['bundle_version'], bundle_version)

    @eventually(30, 1, {botocore.exceptions.ClientError})
    def _get_notification_from_s3_object(self, bucket, key):
        obj = self.s3.get_object(Bucket=bucket, Key=key)['Body'].read().decode("utf-8")
        return json.loads(obj)

    @testmode.integration
    def test_subscription_update(self, replica=Replica.aws):
        """
        Test recover of subscriptions during enumeration
        """
        subscription_1 = self._put_subscription(
            {
                'callback_url': "https://nonsense.or.whatever",
                'method': "PUT",
            },
            replica
        )
        subscription_2 = self._put_subscription(
            {
                'callback_url': "https://nonsense.or.whatever",
                'method': "PUT",
            },
            replica
        )
        url = str(UrlBuilder()
                  .set(path="/v1/subscriptions")
                  .add_query("replica", replica.name)
                  .add_query("subscription_type", "jmespath"))
        resp = self.assertGetResponse(
            url,
            requests.codes.ok,
            headers=get_auth_header())
        subs = {sub['uuid']: sub for sub in json.loads(resp.body)['subscriptions']}
        self.assertIn(subscription_1['uuid'], subs)
        self.assertIn(subscription_2['uuid'], subs)
        for key in subscription_1:
            self.assertEquals(subscription_1[key], subs[subscription_1['uuid']][key])
        for key in subscription_2:
            self.assertEquals(subscription_2[key], subs[subscription_2['uuid']][key])

    @testmode.integration
    def test_subscription_enumerate(self, replica=Replica.aws):
        """
        Test recover of subscriptions during enumeration
        """
        subscription_1 = self._put_subscription(
            {
                'callback_url': "https://nonsense.or.whatever",
                'method': "PUT",
            },
            replica
        )
        subscription_2 = self._put_subscription(
            {
                'callback_url': "https://nonsense.or.whatever",
                'method': "PUT",
            },
            replica
        )
        url = str(UrlBuilder()
                  .set(path="/v1/subscriptions")
                  .add_query("replica", replica.name)
                  .add_query("subscription_type", "jmespath"))
        resp = self.assertGetResponse(
            url,
            requests.codes.ok,
            headers=get_auth_header())
        subs = {sub['uuid']: sub for sub in json.loads(resp.body)['subscriptions']}
        with self.subTest("Test user should own every returned subscription"):
            for sub in subs.values():
                self.assertEquals(self.owner, sub['owner'])
        with self.subTest("Test subscriptions shuold have been returned"):
            self.assertIn(subscription_1['uuid'], subs)
            self.assertIn(subscription_2['uuid'], subs)

    @testmode.integration
    def test_get_subscriptions_for_replica(self):
        for replica in Replica:
            with self.subTest(replica.name):
                subscriptions_v2.get_subscriptions_for_replica(replica)

    @testmode.integration
    def test_subscription_api(self):
        for replica in Replica:
            with self.subTest(replica.name):
                self._test_subscription_api(replica)

    def _test_subscription_api(self, replica: Replica):
        with self.subTest(f"{replica}, Should not be able to PUT with invalid JMESPath"):
            doc = {
                'callback_url': "https://example.com",
                'method': "POST",
                'encoding': "application/json",
                'form_fields': {'foo': "bar"},
                'payload_form_field': "baz",
                'jmespath_query': "not valid JMESPath",
            }
            self._put_subscription(doc, replica, codes=requests.codes.unprocessable)

        with self.subTest(f"{replica}, PUT should succeed for valid JMESPath"):
            sub = {
                'callback_url': "https://example.com",
                'method': "POST",
                'encoding': "application/json",
                'form_fields': {'foo': "bar"},
                'payload_form_field': "baz",
                'jmespath_query': "foo",
            }
            self._put_subscription(sub, replica)

        with self.subTest(f"{replica}, PUT should succceed for missing JMESPath"):
            sub = {
                'callback_url': "https://example.com",
                'method': "POST",
                'encoding': "application/json",
                'form_fields': {'foo': "bar"},
                'payload_form_field': "baz",
            }
            subscription = self._put_subscription(sub, replica)

        with self.subTest(f"{replica}, GET should succeed"):
            sub = self._get_subscription(subscription['uuid'], replica)
            self.assertEquals(sub['uuid'], subscription['uuid'])

        with self.subTest(f"{replica}, DELETE should fail for un-owned subscription"):
            self._delete_subscription(
                subscription['uuid'],
                replica,
                codes=requests.codes.unauthorized,
                use_auth=False
            )

        with self.subTest(f"{replica}, DELETE should succeed"):
            self._delete_subscription(subscription['uuid'], replica)

        with self.subTest(f"{replica}, DELETE on non-existent subscription should return not-found"):
            self._delete_subscription(str(uuid4()), replica, codes=404)

    @testmode.standalone
    def test_should_notify(self):
        """
        Test logic of dss.events.handlers.should_notify()
        """
        for replica in Replica:
            with self.subTest(replica.name):
                self._test_should_notify(replica)

    def _test_should_notify(self, replica):
        with self.subTest("Should not notify when jmespath_query does not match document"):
            sub = self.subscription.copy()
            sub['jmespath_query'] = 'files."assay.json"[?rna.primer==`george`]'
            self.assertFalse(notify_v2.should_notify(replica, sub, "CREATE", self.bundle_key))

        with self.subTest("Should not notify when jmespath_query contains malformed JMESPath"):
            sub = self.subscription.copy()
            sub['jmespath_query'] = 'files."assay.json"[?rna.primer==`george`'
            self.assertFalse(notify_v2.should_notify(replica, sub, "CREATE", self.bundle_key))

    @testmode.standalone
    def test_notify(self):
        with self.subTest("success"):
            self.assertTrue(notify_v2.notify(self.subscription, "CREATE", self.bundle_key))

        with self.subTest("test notification delivery with auth"):
            sub = self.subscription.copy()
            sub['callback_url'] = sub['callback_url'] + "_with_auth"
            sub['hmac_secret_key'] = "ribos0me"
            self.assertTrue(notify_v2.notify(sub, "CREATE", self.bundle_key))

        with self.subTest("test multipart/form-data"):
            sub = self.subscription.copy()
            sub['encoding'] = "multipart/form-data"
            self.assertTrue(notify_v2.notify(sub, "CREATE", self.bundle_key))

        with self.subTest("Test delivery failure"):
            sub = self.subscription.copy()
            sub['callback_url'] = f"http://127.0.0.1:{self.app._port}/notification_test_fail"
            self.assertFalse(notify_v2.notify(sub, "CREATE", self.bundle_key))

    def _put_subscription(self, doc, replica=Replica.aws, codes=requests.codes.created):
        url = str(UrlBuilder()
                  .set(path="/v1/subscriptions")
                  .add_query("replica", replica.name))
        resp = self.assertPutResponse(
            url,
            codes,
            json_request_body=doc,
            headers=get_auth_header()
        )
        return json.loads(resp.body)

    @eventually(5, 1)
    def _get_subscription(self, uuid: str, replica: Replica):
        url = str(UrlBuilder()
                  .set(path=f"/v1/subscriptions/{uuid}")
                  .add_query("replica", replica.name)
                  .add_query("subscription_type", "jmespath"))
        resp = self.assertGetResponse(url, requests.codes.ok, headers=get_auth_header())
        return json.loads(resp.body)

    def _delete_subscription(self, uuid: str, replica: Replica, codes=requests.codes.ok, use_auth=True):
        url = str(UrlBuilder()
                  .set(path=f"/v1/subscriptions/{uuid}")
                  .add_query("replica", replica.name)
                  .add_query("subscription_type", "jmespath"))
        if use_auth:
            resp = self.assertDeleteResponse(url, codes, headers=get_auth_header())
        else:
            resp = self.assertDeleteResponse(url, codes)
        return json.loads(resp.body)

    def _upload_bundle(self, replica, uuid=None):
        if replica == Replica.aws:
            test_fixtures_bucket = get_env('DSS_S3_BUCKET_TEST_FIXTURES')
        else:
            test_fixtures_bucket = get_env('DSS_GS_BUCKET_TEST_FIXTURES')
        bundle_uuid = uuid if uuid else str(uuid4())
        file_uuid_1 = str(uuid4())
        file_uuid_2 = str(uuid4())
        filenames = ["file_1", "file_2"]
        resp_obj_1 = self.upload_file_wait(
            f"{replica.storage_schema}://{test_fixtures_bucket}/test_good_source_data/0",
            replica,
            file_uuid_1,
            bundle_uuid=bundle_uuid,
        )
        resp_obj_2 = self.upload_file_wait(
            f"{replica.storage_schema}://{test_fixtures_bucket}/test_good_source_data/1",
            replica,
            file_uuid_2,
            bundle_uuid=bundle_uuid,
        )
        file_version_1 = resp_obj_1.json['version']
        file_version_2 = resp_obj_2.json['version']
        bundle_version = datetime_to_version_format(datetime.datetime.utcnow())
        self.put_bundle(
            replica,
            bundle_uuid,
            [(file_uuid_1, file_version_1, filenames[0]), (file_uuid_2, file_version_2, filenames[1])],
            bundle_version,
        )
        return bundle_uuid, bundle_version

    def put_bundle(
            self,
            replica: Replica,
            bundle_uuid: str,
            files: typing.Iterable[typing.Tuple[str, str, str]],
            bundle_version: typing.Optional[str] = None,
            expected_code: int = requests.codes.created):
        builder = UrlBuilder().set(path="/v1/bundles/" + bundle_uuid).add_query("replica", replica.name)
        if bundle_version:
            builder.add_query("version", bundle_version)
        url = str(builder)

        resp_obj = self.assertPutResponse(
            url,
            expected_code,
            json_request_body=dict(
                files=[
                    dict(
                        uuid=file_uuid,
                        version=file_version,
                        name=file_name,
                        indexed=False,
                    )
                    for file_uuid, file_version, file_name in files
                ],
                creator_uid=12345,
            ),
            headers=get_auth_header()
        )

        if 200 <= resp_obj.response.status_code < 300:
            self.assertHeaders(
                resp_obj.response,
                {
                    'content-type': "application/json",
                }
            )
            self.assertIn('version', resp_obj.json)
            self.assertIn('manifest', resp_obj.json)
        return resp_obj

    def _tombstone_bundle(self, replica: Replica, bundle_uuid: str, bundle_version: str=None):
        builder = UrlBuilder().set(path="/v1/bundles/" + bundle_uuid).add_query("replica", replica.name)
        if bundle_version:
            builder.add_query("version", bundle_version)
        url = str(builder)
        self.assertDeleteResponse(
            url,
            requests.codes.ok,
            json_request_body={
                'reason': "notification test"
            },
            headers=get_auth_header()
        )

if __name__ == '__main__':
    unittest.main()
