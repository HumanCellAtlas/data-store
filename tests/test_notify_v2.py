#!/usr/bin/env python
# coding: utf-8

import os
import sys
import json
from uuid import uuid4
import boto3
import botocore.exceptions
import requests
from requests_http_signature import HTTPSignatureAuth
import unittest
from unittest import mock
import datetime
import typing
from copy import deepcopy

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.util import UrlBuilder
from dss.events.handlers import notify_v2
from tests.infra import DSSAssertMixin, DSSUploadMixin, get_env, testmode
from dss.config import Replica
from tests.infra.server import ThreadedLocalServer, SilentHandler
from tests import eventually, get_auth_header
from dcplib.aws.sqs import SQSMessenger, get_queue_url
from dss.events.handlers.notify_v2 import notify_or_queue
from dss.util.version import datetime_to_version_format
from dss.subscriptions_v2 import (delete_subscription, get_subscriptions_for_replica, get_subscriptions_for_owner,
                                  SubscriptionData)


recieved_notification = None


class MyHandlerClass(SilentHandler):
    """
    Modify ThreadedLocalServer to respond to our notification deliveries.
    """
    hmac_secret_key = "ribos0me"
    def my_handle(self, method):
        global recieved_notification
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
        size = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(size)
        try:
            recieved_notification = json.loads(body)
        except Exception as e:
            recieved_notification = str(e)
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
        cls.s3 = boto3.client('s3')

    @classmethod
    def tearDownClass(cls):
        for replica in Replica:
            subs = [s for s in get_subscriptions_for_owner(replica, cls.owner) if s['owner'] == cls.owner]
            for s in subs:
                delete_subscription(replica, cls.owner, s['uuid'])

    @testmode.integration
    def test_regex_patterns(self):
        version = datetime_to_version_format(datetime.datetime.utcnow())
        key = f"bundles/{uuid4()}.{version}"
        tombstone_key_with_version = key + ".dead"
        tombstone_key_without_version = f"bundles/{uuid4()}.dead"

        self.assertIsNone(notify_v2._versioned_tombstone_key_regex.match(key))
        self.assertIsNone(notify_v2._unversioned_tombstone_key_regex.match(key))

        self.assertIsNotNone(notify_v2._versioned_tombstone_key_regex.match(tombstone_key_with_version))
        self.assertIsNone(notify_v2._versioned_tombstone_key_regex.match(tombstone_key_without_version))

        self.assertIsNone(notify_v2._unversioned_tombstone_key_regex.match(tombstone_key_with_version))
        self.assertIsNotNone(notify_v2._unversioned_tombstone_key_regex.match(tombstone_key_without_version))

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
        self.assertEquals(notification['match']['bundle_version'], f"{bundle_version}")

    @testmode.standalone
    def test_notify_or_queue(self):
        replica = Replica.aws
        metadata_document = dict()
        subscription = {
            SubscriptionData.REPLICA: replica,
            SubscriptionData.OWNER: "bob",
            SubscriptionData.UUID: str(uuid4()),
        }

        with self.subTest("Should attempt to notify immediately"):
            with mock.patch("dss.events.handlers.notify_v2.notify") as mock_notify:
                with mock.patch.object(SQSMessenger, "send") as mock_send:
                    notify_or_queue(replica, subscription, metadata_document, "bundles/some_uuid")
                    mock_notify.assert_called()
                    mock_send.assert_not_called()

        with self.subTest("Should queue when notify fails"):
            with mock.patch("dss.events.handlers.notify_v2.notify") as mock_notify:
                mock_notify.return_value = False
                with mock.patch.object(SQSMessenger, "send", mock_send):
                    notify_or_queue(Replica.aws, subscription, metadata_document, "bundles/some_uuid")
                    mock_notify.assert_called()
                    mock_send.assert_called()

        with self.subTest("notify_or_queue should attempt to notify immediately for versioned tombsstone"):
            with mock.patch("dss.events.handlers.notify_v2.notify") as mock_notify:
                with mock.patch("dss.events.handlers.notify_v2._list_prefix") as mock_list_prefix:
                    bundle_uuid = str(uuid4())
                    bundle_version = datetime_to_version_format(datetime.datetime.utcnow())
                    key = f"bundles/{bundle_uuid}.{bundle_version}"
                    mock_list_prefix.return_value = [key]
                    notify_or_queue(Replica.aws, subscription, metadata_document, key + ".dead")
                    mock_notify.assert_called_with(subscription, metadata_document, key)

        with self.subTest("notify_or_queue should queue notifications for unversioned tombsstone"):
            bundle_uuid = str(uuid4())
            bundle_version_1 = datetime_to_version_format(datetime.datetime.utcnow())
            bundle_version_2 = datetime_to_version_format(datetime.datetime.utcnow())
            bundle_key_1 = f"bundles/{bundle_uuid}.{bundle_version_1}"
            bundle_key_2 = f"bundles/{bundle_uuid}.{bundle_version_2}"
            unversioned_tombstone_key = f"bundles/{bundle_uuid}.dead"

            with mock.patch.object(SQSMessenger, "send") as mock_send:
                with mock.patch("dss.events.handlers.notify_v2._list_prefix") as mock_list_prefix:
                    mock_list_prefix.return_value = [
                        unversioned_tombstone_key,
                        bundle_key_1,
                        bundle_key_2,
                    ]
                    notify_or_queue(Replica.aws, subscription, metadata_document, unversioned_tombstone_key)
                    keys = [json.loads(a[0][0])['key'] for a in mock_send.call_args_list]
                    self.assertIn(bundle_key_1, keys)
                    self.assertIn(bundle_key_2, keys)

        with self.subTest("notify_or_queue should not re-queue tombstones versions of unversioned tombstones"):
            bundle_uuid = str(uuid4())
            bundle_version_1 = datetime_to_version_format(datetime.datetime.utcnow())
            bundle_version_2 = datetime_to_version_format(datetime.datetime.utcnow())
            bundle_key_1 = f"bundles/{bundle_uuid}.{bundle_version_1}"
            bundle_key_2 = f"bundles/{bundle_uuid}.{bundle_version_2}.dead"
            unversioned_tombstone_key = f"bundles/{bundle_uuid}.dead"
            with mock.patch.object(SQSMessenger, "send") as mock_send:
                with mock.patch("dss.events.handlers.notify_v2._list_prefix") as mock_list_prefix:
                    mock_list_prefix.return_value = [
                        unversioned_tombstone_key,
                        bundle_key_1,
                        bundle_key_2,
                    ]
                    notify_or_queue(Replica.aws, subscription, metadata_document, unversioned_tombstone_key)
                    mock_send.assert_called_once()
                    keys = [json.loads(a[0][0])['key'] for a in mock_send.call_args_list]
                    self.assertIn(bundle_key_1, keys)

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

        with SQSMessenger(get_queue_url(notify_v2.notification_queue_name)) as mq:
            msg = notify_v2._format_sqs_message(
                replica,
                subscription,
                "CREATE",
                "bundles/a47b90b2-0967-4fbf-87bc-c6c12db3fedf.2017-07-12T055120.037644Z",
            )
            mq.send(msg, delay_seconds=0)
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
        # There's a chance an unrelated bundle will trigger our subscription
        # self.assertEquals(notification['match']['bundle_uuid'], bundle_uuid)
        # self.assertEquals(notification['match']['bundle_version'], bundle_version)

    @eventually(60, 1, {botocore.exceptions.ClientError})
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
                  .add_query("replica", replica.name))
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
                  .add_query("replica", replica.name))
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
                get_subscriptions_for_replica(replica)

    @testmode.integration
    def test_subscription_api(self):
        """
        Test PUT, GET, DELETE subscription endpoints
        This hits the test API server, however it writes changes to the deployed subscriptions dynamodb table
        """
        for replica in Replica:
            with self.subTest(replica.name):
                self._test_subscription_api(replica)

    def _test_subscription_api(self, replica: Replica):
        subscription_doc = {
            'callback_url': "https://example.com",
            'method': "POST",
            'encoding': "application/json",
            'form_fields': {'foo': "bar"},
            'payload_form_field': "baz",
            'hmac_key_id': 'test_notify_v2',
            'hmac_secret_key': '2333',
            'attachments': {
                "my_attachment_1": {
                    'type': "jmespath",
                    'expression': "this.is.valid.jmespath"
                },
                "my_attachment_2": {
                    'type': "jmespath",
                    'expression': "this.is.valid.jmespath"
                }
            }
        }
        with self.subTest(f"{replica}, PUT should succceed for missing JMESPath"):
            doc = deepcopy(subscription_doc)
            subscription = self._put_subscription(doc, replica)

        with self.subTest(f"{replica}, Should not be able to PUT with invalid JMESPath"):
            doc = deepcopy(subscription_doc)
            doc['jmespath_query'] = "this is not valid jmespath"
            resp = self._put_subscription(doc, replica, codes=requests.codes.bad_request)
            self.assertEquals("invalid_jmespath", resp['code'])

        with self.subTest(f"{replica}, PUT should NOT succeed for attachment name starting with '_'"):
            doc = deepcopy(subscription_doc)
            doc['attachments']['_illegal_attachment_name'] = doc['attachments']['my_attachment_1']  # type: ignore
            resp = self._put_subscription(doc, replica, codes=requests.codes.bad_request)
            self.assertEquals("invalid_attachment_name", resp['code'])

        with self.subTest(f"{replica}, PUT should NOT succeed for invalid attachment JMESPath"):
            doc = deepcopy(subscription_doc)
            doc['attachments']['my_attachment_1']['expression'] = "this is not valid jmespath"  # type: ignore
            resp = self._put_subscription(doc, replica, codes=requests.codes.bad_request)
            self.assertEquals("invalid_attachment_expression", resp['code'])

        with self.subTest(f"{replica}, PUT should succeed for valid JMESPath"):
            doc = deepcopy(subscription_doc)
            doc['jmespath_query'] = "foo"
            self._put_subscription(doc, replica)

        with self.subTest(f"{replica}, GET should succeed"):
            sub = self._get_subscription(subscription['uuid'], replica)
            self.assertEquals(sub['uuid'], subscription['uuid'])

        with self.subTest(f"{replica}, hmac_secret_key should not be present, hmac_key_id should be found"):
            sub = self._get_subscription(subscription['uuid'], replica)
            self.assertEquals(sub['uuid'], subscription['uuid'])
            self.assertNotIn('hmac_secret_ket', sub)
            self.assertEquals(sub['hmac_key_id'], subscription['hmac_key_id'])

        with self.subTest(f"{replica}, DELETE should fail for un-owned subscription"):
            sub = self._get_subscription(subscription['uuid'], replica)
            self._delete_subscription(sub['uuid'], replica, codes=requests.codes.unauthorized, use_auth=False)

        with self.subTest(f"{replica}, DELETE should succeed"):
            sub = self._get_subscription(subscription['uuid'], replica)
            self._delete_subscription(sub['uuid'], replica)

        with self.subTest(f"{replica}, DELETE on non-existent subscription should return not-found"):
            self._delete_subscription(str(uuid4()), replica, codes=404)

    @testmode.integration
    def test_should_notify(self):
        """
        Test logic of dss.events.handlers.should_notify()
        """
        for replica in Replica:
            with self.subTest(replica.name):
                self._test_should_notify(replica)

    def _test_should_notify(self, replica):
        bundle_uuid, bundle_version = self._shared_bundle_once(replica)
        bundle_key = f"bundles/{bundle_uuid}.{bundle_version}"
        subscription = {
            'owner': self.owner,
            'uuid': str(uuid4()),
            'callback_url': f"http://127.0.0.1:{self.app._port}/notification_test_pass",
            'method': "POST",
            'encoding': "application/json",
            'form_fields': {'foo': "bar"},
            'payload_form_field': "baz",
            'replica': "aws",
            'jmespath_query': 'files."assay.json"[?rna.primer==`random`]',
        }

        metadata_doc = {
            'event_type': "CREATE",
        }

        with self.subTest("Should not notify when jmespath_query does not match document"):
            sub = deepcopy(subscription)
            sub['jmespath_query'] = 'files."assay.json"[?rna.primer==`george`]'
            self.assertFalse(notify_v2.should_notify(replica, sub, metadata_doc, bundle_key))

        with self.subTest("Should not notify when jmespath_query contains malformed JMESPath"):
            sub = deepcopy(subscription)
            sub['jmespath_query'] = 'files."assay.json"[?rna.primer==`george`'
            self.assertFalse(notify_v2.should_notify(replica, sub, metadata_doc, bundle_key))

    @testmode.integration
    def test_notify(self):
        self._test_notify({'event_type': "CREATE"})
        self._test_notify({'event_type': "TOMBSTONE"})
        self._test_notify({'event_type': "DELETE"})

    def _test_notify(self, metadata_document):
        api_name = f"https://{os.getenv('API_DOMAIN_NAME')}"
        bundle_uuid, bundle_version = self._shared_bundle_once(Replica.aws)
        bundle_key = f"bundles/{bundle_uuid}.{bundle_version}"
        subscription = {
            'owner': self.owner,
            'uuid': str(uuid4()),
            'callback_url': f"http://127.0.0.1:{self.app._port}/notification_test_pass",
            'method': "POST",
            'encoding': "application/json",
            'form_fields': {'foo': "bar"},
            'payload_form_field': "baz",
            'replica': "aws",
            'jmespath_query': 'files."assay.json"[?rna.primer==`random`]',
        }

        metadata_doc = {
            'event_type': "CREATE",
        }


        with self.subTest("success"):
            sub = deepcopy(subscription)
            self.assertTrue(notify_v2.notify(sub, metadata_doc, bundle_key))

        with self.subTest("Delivery should succeed using hmac_secret_key"):
            sub = deepcopy(subscription)
            sub['callback_url'] = sub['callback_url'] + "_with_auth"
            sub['hmac_secret_key'] = "ribos0me"
            self.assertTrue(notify_v2.notify(sub, metadata_doc, bundle_key))

        with self.subTest("Delivery should succeed with multipart/form-data"):
            sub = deepcopy(subscription)
            sub['encoding'] = "multipart/form-data"
            self.assertTrue(notify_v2.notify(sub, metadata_doc, bundle_key))

        with self.subTest("Notify should return False when delivery fails"):
            sub = deepcopy(subscription)
            sub['callback_url'] = f"http://127.0.0.1:{self.app._port}/notification_test_fail"
            self.assertFalse(notify_v2.notify(sub, metadata_doc, bundle_key))

        with self.subTest("Notify should return False when delivery fails"):
            sub = deepcopy(subscription)
            sub['callback_url'] = f"http://127.0.0.1:{self.app._port}/notification_test_fail"
            self.assertFalse(notify_v2.notify(sub, metadata_doc, bundle_key))

        with self.subTest("Test notification with matching attachments"):
            sub = deepcopy(subscription)
            metadata_doc = {
                'event_type': "CREATE",
                'foo': "george",
                'bar': "Frank"
            }
            sub['attachments'] = {
                'my_attachment_1': {
                    'type': "jmespath",
                    'expression': "foo"
                },
                'my_attachment_2': {
                    'type': "jmespath",
                    'expression': "bar"
                },
                'my_attachment_3': {
                    'type': "jmespath",
                    'expression': "blarg.arg"
                }
            }
            self.assertTrue(notify_v2.notify(sub, metadata_doc, bundle_key))
            self.assertEqual(recieved_notification['attachments']['my_attachment_1'], metadata_doc['foo'])
            self.assertEqual(recieved_notification['attachments']['my_attachment_2'], metadata_doc['bar'])
            self.assertEqual(recieved_notification['attachments']['my_attachment_3'], None)
            self.assertEquals(api_name, recieved_notification['dss_api'])
            self.assertIn('bundle_url', recieved_notification)
            self.assertIn('event_timestamp', recieved_notification)

        with self.subTest("Test notification with no attachments"):
            sub = deepcopy(subscription)
            metadata_doc = {
                'event_type': "CREATE",
            }
            self.assertTrue(notify_v2.notify(sub, metadata_doc, bundle_key))
            self.assertEqual(recieved_notification.get('attachments'), None)
            self.assertEquals(api_name, recieved_notification['dss_api'])
            self.assertIn('bundle_url', recieved_notification)
            self.assertIn('event_timestamp', recieved_notification)

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
                  .add_query("replica", replica.name))
        resp = self.assertGetResponse(url, requests.codes.ok, headers=get_auth_header())
        return json.loads(resp.body)

    def _delete_subscription(self, uuid: str, replica: Replica, codes=requests.codes.ok, use_auth=True):
        url = str(UrlBuilder()
                  .set(path=f"/v1/subscriptions/{uuid}")
                  .add_query("replica", replica.name))
        if use_auth:
            resp = self.assertDeleteResponse(url, codes, headers=get_auth_header())
        else:
            resp = self.assertDeleteResponse(url, codes)
        return json.loads(resp.body)

    def _shared_bundle_once(self, replica: Replica):
        """
        Upload a shared test bundle
        """
        cls = type(self)
        uuid_key = f"_bundle_uuid_once_{replica.name}"
        version_key = f"_bundle_version_once_{replica.name}"
        if not getattr(cls, uuid_key, None):
            uuid, version = self._upload_bundle(replica)
            setattr(cls, uuid_key, uuid)
            setattr(cls, version_key, version)
        return getattr(cls, uuid_key), getattr(cls, version_key)

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
