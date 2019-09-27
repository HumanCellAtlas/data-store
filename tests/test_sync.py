#!/usr/bin/env python
# coding: utf-8

import base64
import datetime
import io
import logging
import os
import random
import sys
import string
import json
import hashlib
import unittest
from unittest import mock
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import flask
import boto3
import crcmod
from botocore.vendored import requests
from dcplib.s3_multipart import MULTIPART_THRESHOLD, get_s3_multipart_chunk_size
from dcplib.checksumming_io import ChecksummingSink
from google.cloud.exceptions import NotFound

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import importlib
import dss
from dss.api import bundles as bundles_api
from dss.api import files as files_api
from dss.config import Config, Replica
from dss.events.handlers import sync
from dss.logging import configure_test_logging
from dss.util import UrlBuilder
from dss.util.streaming import get_pool_manager, S3SigningChunker
from dss.storage.identifiers import FILE_PREFIX, BUNDLE_PREFIX, COLLECTION_PREFIX, FileFQID, BundleFQID,\
    CollectionFQID, BLOB_KEY_REGEX
from dss.storage.hcablobstore import BundleFileMetadata, BundleMetadata, FileMetadata, compose_blob_key
from tests import get_auth_header
from tests.infra.server import ThreadedLocalServer
from tests import eventually, get_version, get_collection_fqid
from tests.infra import testmode

daemon_app = importlib.import_module('daemons.dss-sync-sfn.app')


def setUpModule():
    configure_test_logging()


def generate_random_blob_key():
    random_uuid = str(uuid.uuid4())
    sha1 = hashlib.sha1(str.encode(random_uuid)).hexdigest()
    sha256 = hashlib.sha256(str.encode(random_uuid)).hexdigest()
    md5 = hashlib.md5(str.encode(random_uuid)).hexdigest()
    crc = ''.join(random.choice(string.digits) for x in range(8))
    return f"{sha256}.{sha1}.{md5}.{crc}"


class DSSSyncMixin:
    test_blob_prefix = {"gs": f"blobs/{generate_random_blob_key()}",
                        "s3": f"blobs/{generate_random_blob_key()}",
                        "metadata_gs": f"blobs/{generate_random_blob_key()}",
                        "metadata_s3": f"blobs/{generate_random_blob_key()}",
                        "streaming_s3": f"blobs/{generate_random_blob_key()}",
                        "compose_gs_blobs": f"blobs/{generate_random_blob_key()}",
                        "copy_part_s3": f"blobs/{generate_random_blob_key()}",
                        "copy_part_gs": f"blobs/{generate_random_blob_key()}",
                        "exists_s3": f"blobs/{generate_random_blob_key()}",
                        "exists_gs": f"blobs/{generate_random_blob_key()}"}

    def cleanup_sync_test_objects(self, age=datetime.timedelta(days=1)):
        for replica, blob_key_prefix in self.test_blob_prefix.items():
            for key in self.s3_bucket.objects.filter(Prefix=blob_key_prefix):
                if key.last_modified < datetime.datetime.now(datetime.timezone.utc) - age:
                    key.delete()
            for key in self.gs_bucket.list_blobs(prefix=blob_key_prefix):
                if key.time_created < datetime.datetime.now(datetime.timezone.utc) - age:
                    try:
                        key.delete()
                    except NotFound:
                        pass

    payload = b''

    def get_payload(self, size):
        if len(self.payload) < size:
            self.payload += os.urandom(size - len(self.payload))
        return self.payload[:size]

    def _assert_content_type(self, s3_blob, gs_blob):
        gs_blob.reload()
        self.assertEqual(s3_blob.content_type, gs_blob.content_type)


@testmode.integration
class TestSyncUtils(unittest.TestCase, DSSSyncMixin):
    def setUp(self):
        dss.Config.set_config(dss.BucketConfig.TEST)
        self.gs_bucket_name, self.s3_bucket_name = dss.Config.get_gs_bucket(), dss.Config.get_s3_bucket()
        self.logger = logging.getLogger(__name__)
        self.gs = Config.get_native_handle(Replica.gcp)
        self.gs_bucket = self.gs.bucket(self.gs_bucket_name)
        self.s3 = boto3.resource("s3")
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)

    def test_sync_blob(self):
        self.cleanup_sync_test_objects()
        payload = self.get_payload(2**20)
        test_metadata = {"metadata-sync-test": str(uuid.uuid4())}
        test_key = self.test_blob_prefix["metadata_s3"]
        src_blob = self.s3_bucket.Object(test_key)
        gs_dest_blob = self.gs_bucket.blob(test_key)
        src_blob.put(Body=payload, Metadata=test_metadata)
        source = sync.BlobLocation(platform="s3", bucket=self.s3_bucket, blob=src_blob)
        dest = sync.BlobLocation(platform="gs", bucket=self.gs_bucket, blob=gs_dest_blob)
        sync.sync_s3_to_gs_oneshot(source, dest)
        sync.do_oneshot_copy(Replica.aws, Replica.gcp, test_key)
        self.assertEqual(gs_dest_blob.download_as_string(), payload)
        self._assert_content_type(source.blob, dest.blob)

        gs_dest_blob.reload()
        self.assertEqual(gs_dest_blob.metadata, test_metadata)

        test_key = self.test_blob_prefix["metadata_gs"]
        src_blob = self.gs_bucket.blob(test_key)
        dest_blob = self.s3_bucket.Object(test_key)
        src_blob.metadata = test_metadata
        src_blob.upload_from_string(payload)
        source = sync.BlobLocation(platform="gs", bucket=self.gs_bucket, blob=src_blob)
        dest = sync.BlobLocation(platform="s3", bucket=self.s3_bucket, blob=dest_blob)
        sync.sync_gs_to_s3_oneshot(source, dest)
        sync.do_oneshot_copy(Replica.gcp, Replica.aws, test_key)
        self.assertEqual(dest_blob.get()["Body"].read(), payload)
        self.assertEqual(dest_blob.metadata, test_metadata)
        self._assert_content_type(dest.blob, source.blob)

        # Hit some code paths with mock data. The full tests for these are in the integration test suite.
        sync.initiate_multipart_upload(Replica.gcp, Replica.aws, test_key)
        sync.get_sync_work_state(dict(source_replica="aws",
                                      dest_replica="gcp",
                                      source_key=test_key,
                                      source_obj_metadata=dict(size=0)))

    def test_s3_streaming(self):
        boto3_session = boto3.session.Session()
        payload = io.BytesIO(self.get_payload(2**20))
        test_key = self.test_blob_prefix['streaming_s3']
        chunker = S3SigningChunker(fh=payload,
                                   total_bytes=len(payload.getvalue()),
                                   credentials=boto3_session.get_credentials(),
                                   service_name="s3",
                                   region_name=boto3_session.region_name)
        upload_url = "{host}/{bucket}/{key}".format(host=self.s3.meta.client.meta.endpoint_url,
                                                    bucket=self.s3_bucket.name,
                                                    key=test_key)
        res = get_pool_manager().request("PUT", upload_url,
                                         headers=chunker.get_headers("PUT", upload_url),
                                         body=chunker,
                                         chunked=True,
                                         retries=False)
        self.assertEqual(res.status, requests.codes.ok)
        self.assertEqual(self.s3_bucket.Object(test_key).get()["Body"].read(), payload.getvalue())

    def test_compose_gs_blobs(self):
        test_key = self.test_blob_prefix['compose_gs_blobs']
        blob_names = []
        total_payload = b""
        for part in range(3):
            payload = self.get_payload(2**10)
            self.gs_bucket.blob(f"{test_key}.part{part}").upload_from_string(payload)
            blob_names.append(f"{test_key}.part{part}")
            total_payload += payload
        sync.compose_gs_blobs(self.gs_bucket, blob_names, test_key)
        self.assertEqual(self.gs_bucket.blob(test_key).download_as_string(), total_payload)
        for part in range(3):
            self.assertFalse(self.gs_bucket.blob(f"{test_key}.part{part}").exists())

    def test_copy_part_s3_to_gs(self):
        payload = self.get_payload(2**20)
        test_key = self.test_blob_prefix['copy_part_s3']
        test_blob = self.s3_bucket.Object(test_key)
        test_blob.put(Body=payload)
        source_url = self.s3.meta.client.generate_presigned_url("get_object",
                                                                Params=dict(Bucket=self.s3_bucket.name, Key=test_key))
        part = dict(start=0, end=len(payload) - 1)
        upload_url = self.gs_bucket.blob(test_key).create_resumable_upload_session(size=len(payload))
        res = sync.copy_part(upload_url, source_url, dest_platform="gs", part=part)
        crc = crcmod.predefined.Crc('crc-32c')
        crc.update(payload)
        self.assertEqual(base64.b64decode(json.loads(res.content)["crc32c"]), crc.digest())

    def test_copy_part_gs_to_s3(self):
        payload = self.get_payload(2**20)
        test_key = self.test_blob_prefix['copy_part_gs']
        test_blob = self.gs_bucket.blob(test_key)
        test_blob.upload_from_string(payload)
        source_url = test_blob.generate_signed_url(datetime.timedelta(hours=1))
        part = dict(start=0, end=2**20 - 1)
        upload_url = "{host}/{bucket}/{key}".format(host=self.s3.meta.client.meta.endpoint_url,
                                                    bucket=self.s3_bucket.name,
                                                    key=test_key)
        res = sync.copy_part(upload_url, source_url, dest_platform="s3", part=part)
        self.assertEqual(json.loads(res.headers["ETag"]), hashlib.md5(payload).hexdigest())

    def test_exists(self):
        with self.subTest("gs"):
            test_key = self.test_blob_prefix['exists_gs']
            test_blob = self.gs_bucket.blob(test_key)
            self.assertFalse(sync.exists(replica=Replica.gcp, key=test_key))
            test_blob.upload_from_string(b"1")
            self.assertTrue(sync.exists(replica=Replica.gcp, key=test_key))

        with self.subTest("s3"):
            test_key = self.test_blob_prefix['exists_s3']
            test_blob = self.s3_bucket.Object(test_key)
            self.assertFalse(sync.exists(replica=Replica.aws, key=test_key))
            test_blob.put(Body=b"2")
            self.assertTrue(sync.exists(replica=Replica.aws, key=test_key))

    def test_dependencies_exist(self):
        file_uuid, file_version = str(uuid.uuid4()), get_version()
        bundle_uuid, bundle_version = str(uuid.uuid4()), get_version()
        collection_data = {"contents": [
            {"type": "bundle", "uuid": bundle_uuid, "version": bundle_version},
            {"type": "file", "uuid": file_uuid, "version": file_version}
        ]}
        bundle_data = {BundleMetadata.FILES: [
            {BundleFileMetadata.UUID: file_uuid, BundleFileMetadata.VERSION: file_version}
        ]}
        file_data = {
            FileMetadata.SHA256: "sync_test",
            FileMetadata.SHA1: "sync_test",
            FileMetadata.S3_ETAG: "sync_test",
            FileMetadata.CRC32C: str(uuid.uuid4())
        }

        with self.subTest("collection without deps"):
            collection_key = "{}/{}".format(COLLECTION_PREFIX, get_collection_fqid())
            collection_blob = self.s3_bucket.Object(collection_key)
            collection_blob.put(Body=json.dumps(collection_data).encode())
            self.assertFalse(sync.dependencies_exist(Replica.aws, Replica.aws, collection_key))

        with self.subTest("bundle without deps"):
            bundle_key = "{}/{}".format(BUNDLE_PREFIX, BundleFQID(uuid=bundle_uuid, version=bundle_version))
            bundle_blob = self.s3_bucket.Object(bundle_key)
            bundle_blob.put(Body=json.dumps(bundle_data).encode())

            self.assertFalse(sync.dependencies_exist(Replica.aws, Replica.aws, collection_key))
            self.assertFalse(sync.dependencies_exist(Replica.aws, Replica.aws, bundle_key))

        with self.subTest("file without deps"):
            file_key = "{}/{}".format(FILE_PREFIX, FileFQID(uuid=file_uuid, version=file_version))
            file_blob = self.s3_bucket.Object(file_key)
            file_blob.put(Body=json.dumps(file_data).encode())

            @eventually(timeout=8, interval=1, errors={Exception})
            def check_file_revdeps():
                self.assertTrue(sync.dependencies_exist(Replica.aws, Replica.aws, collection_key))
                self.assertTrue(sync.dependencies_exist(Replica.aws, Replica.aws, bundle_key))
                self.assertFalse(sync.dependencies_exist(Replica.aws, Replica.aws, file_key))
            check_file_revdeps()

        with self.subTest("blob presence causes all dependencies to be resolved"):
            blob_key = compose_blob_key(file_data)
            blob_blob = self.s3_bucket.Object(blob_key)
            blob_blob.put(Body=b"sync_test")

            @eventually(timeout=8, interval=1, errors={Exception})
            def check_blob_revdeps():
                self.assertTrue(sync.dependencies_exist(Replica.aws, Replica.aws, collection_key))
                self.assertTrue(sync.dependencies_exist(Replica.aws, Replica.aws, bundle_key))
                self.assertTrue(sync.dependencies_exist(Replica.aws, Replica.aws, file_key))
            check_blob_revdeps()

    def test_get_sync_work_state(self):
        event = dict(source_replica=Replica.aws.name,
                     dest_replica=Replica.gcp.name,
                     source_obj_metadata=dict(size=8),
                     source_key="fake",
                     dest_key="fake")
        for part_size, expected_total_parts in [(3, 3), (4, 2), (5, 2)]:
            with mock.patch("dss.events.handlers.sync.get_part_size", lambda object_size, dest_replica: part_size):
                with self.subTest(part_size=part_size, object_size=event['source_obj_metadata']['size']):
                    self.assertEqual(sync.get_sync_work_state(event)['total_parts'], expected_total_parts)

    def test_blob_key_format(self):
        good_blob_key = f"blobs/{generate_random_blob_key()}"
        bad_blob_key = f"blobs/{generate_random_blob_key()[:-1]}"
        missing_prefix = f"{generate_random_blob_key()}"
        gcp_parts = f"blobs/{generate_random_blob_key()}.partA"
        temp_etag_modification = f"blobs/{generate_random_blob_key()}"
        temp_etag_modification = temp_etag_modification.rsplit('.')
        temp_etag_modification[2] = temp_etag_modification[2] + f'-{random.choice([x for x in range(2,10000)])}'
        aws_parts = '.'.join(temp_etag_modification)

        expected_results = {"good_blob_key": (good_blob_key, 7),  # good blobs have 7 parts, (e_tag takes 3)
                            "bad_blob_key": (bad_blob_key, None),
                            "missing_prefix": (missing_prefix, None),
                            "gcp_parts": (gcp_parts, None),
                            "aws_parts": (aws_parts, 7)}

        for k, v in expected_results.items():
            match = BLOB_KEY_REGEX.match(v[0])
            num_groups = len(match.groups()) if match else None
            self.assertEquals(num_groups, v[1])

    def test_skip_part_sync(self):
        random_part_blob = f"blobs/{generate_random_blob_key()}.partc"
        json_message = json.dumps({"bucket": "org-humancellatlas-dss-dev",
                                   "componentCount": 30,
                                   "contentType": "application/octet-stream",
                                   "crc32c": "1n/pig==",
                                   "etag": "CPjBmfqN1+QCEAE=",
                                   "generation": "1568697601122552",
                                   "kind": "storage#object",
                                   "metageneration": "1",
                                   "name": random_part_blob,
                                   "resourceState": "exists",
                                   "selfLink": f"https://www.googleapis.com/storage/v1/b/"
                                   f"org-humancellatlas-dss-staging/o/{random_part_blob}",
                                   "size": "19658416372",
                                   "storageClass": "MULTI_REGIONAL",
                                   "timeCreated": "2019-09-17T05:20:01.122Z",
                                   "timeStorageClassUpdated": "2019-09-17T05:20:01.122Z",
                                   "updated": "2019-09-17T05:20:01.122Z"})
        event = {"Records": [{
                 "source_replica": "gcp",
                 "dest_replica": "aws",
                 "source_key": random_part_blob,
                 "body": json.dumps({"Message": json_message})
                 }]
                 }
        results = daemon_app.launch_from_forwarded_event(event, None)
        self.assertFalse(results)

    @mock.patch("daemons.dss-sync-sfn.app.resources")
    def test_s3_skip_sync(self, mock_resource):
        random_part_blob = f"blobs/{generate_random_blob_key()}.partc"

        class MagicBucket(object):
            name = self.s3_bucket_name

            def __init__(bucket_self, *args):
                pass

            class Object(object):
                key = random_part_blob

                def __init__(self, *args):
                    pass

        event = {"Records": [{
            "s3": {
                "bucket": {"name": self.s3_bucket_name},
                "object": {"key": random_part_blob}
            }
        }]}

        # Note that MagicBucket must provide anything the callee asks of Bucket
        thisBucket = mock_resource.s3.Bucket = mock.MagicMock(return_value=MagicBucket())  # noqa
        results = daemon_app.launch_from_s3_event(event, None)
        self.assertFalse(results)
# TODO: (akislyuk) integration test of SQS fault injection, SFN fault injection


@testmode.integration
class TestSyncDaemon(unittest.TestCase, DSSSyncMixin):
    def setUp(self):
        dss.Config.set_config(dss.BucketConfig.NORMAL)
        self.gs_bucket_name, self.s3_bucket_name = dss.Config.get_gs_bucket(), dss.Config.get_s3_bucket()
        self.logger = logging.getLogger(__name__)
        self.gs = Config.get_native_handle(Replica.gcp)
        self.gs_bucket = self.gs.bucket(self.gs_bucket_name)
        self.s3 = boto3.resource("s3")
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)

    def test_sync_small_blob(self):
        "Tests oneshot blob syncing."
        self._sync_blob()

    @unittest.skipUnless(json.loads(os.environ.get("DSS_TEST_LARGE_FILE_SYNC", "false")),
                         "Skipping large file sync test. Set DSS_TEST_LARGE_FILE_SYNC=1 to run it")
    def test_sync_large_blob(self):
        "Tests multipart blob syncing."
        self._sync_blob(payload_size=max(sync.part_size.values()) - 1)
        self._sync_blob(payload_size=max(sync.part_size.values()))
        self._sync_blob(payload_size=max(sync.part_size.values()) + 1)

    def _sync_blob(self, payload_size=8):
        self.cleanup_sync_test_objects()
        payload = self.get_payload(payload_size)
        test_metadata = {"metadata-sync-test": str(uuid.uuid4())}

        with self.subTest("s3 to gs"):
            test_key = self.test_blob_prefix['s3']
            src_blob = self.s3_bucket.Object(test_key)
            gs_dest_blob = self.gs_bucket.blob(test_key)
            src_blob.put(Body=payload,
                         Metadata=test_metadata,
                         ContentType="binary/octet-stream; dss-s3-to-gs-sync-test")

            @eventually(timeout=60, interval=1, errors={Exception})
            def check_gs_dest():
                self.assertEqual(gs_dest_blob.download_as_string(), payload)
            check_gs_dest()

            gs_dest_blob.reload()
            self.assertEqual(gs_dest_blob.metadata, test_metadata)
            self._assert_content_type(src_blob, gs_dest_blob)

        with self.subTest("gs to s3"):
            test_key = self.test_blob_prefix['gs']
            src_blob = self.gs_bucket.blob(test_key)
            dest_blob = self.s3_bucket.Object(test_key)
            src_blob.metadata = test_metadata
            src_blob.upload_from_string(payload)
            src_blob.content_type = "binary/octet-stream; dss-gs-to-s3-sync-test"
            src_blob.patch()

            @eventually(timeout=60, interval=1, errors={Exception})
            def check_s3_dest(dest_blob):
                self.assertEqual(dest_blob.get()["Body"].read(), payload)
                self.assertEqual(dest_blob.metadata, test_metadata)
            check_s3_dest(dest_blob)
            self._assert_content_type(dest_blob, src_blob)

@testmode.integration
class TestSyncDaemonLargeManifests(unittest.TestCase, DSSSyncMixin):
    @classmethod
    def setUpClass(cls):
        cls.app = ThreadedLocalServer()
        cls.app.start()
        cls.flask_app = flask.Flask(__name__)

    @classmethod
    def teardownClass(cls):
        cls.app.shutdown()

    def setUp(self):
        dss.Config.set_config(dss.BucketConfig.NORMAL)
        self.s3 = boto3.resource("s3")
        self.number_of_files = 1000000

    @unittest.skipUnless(json.loads(os.environ.get("DSS_TEST_LARGE_MANIFEST_SYNC", "false")),
                         "Skipping large bundle manifest sync test. Set DSS_TEST_LARGE_MANIFEST_SYNC=1 to run it")
    def test_sync_large_manifest(self):
        file_url = self._stage_file(str(uuid.uuid4()))
        files = [dict(name=str(uuid.uuid4()), uuid=file_uuid, version=file_version, indexed=False)
                 for file_uuid, file_version in self._put_files(file_url, number_of_files=self.number_of_files)]
        bundle_uuid = str(uuid.uuid4())
        bundle_version = str(datetime.datetime.utcnow().strftime("%Y-%m-%dT%H%M%S.%fZ"))
        with mock.patch.object(bundles_api.security, "request"):
            with mock.patch.object(bundles_api.security, "assert_authorized_group"):
                with mock.patch.object(bundles_api, "nestedcontext"):
                    with self.flask_app.test_request_context('/'):
                        bundles_api.put(bundle_uuid, "aws", dict(creator_uid=1, files=files), bundle_version)

        @eventually(timeout=600, interval=10, errors={AssertionError})
        def check_dest():
            url = str(UrlBuilder().set(path='/v1/bundles/' + bundle_uuid)
                      .add_query("version", bundle_version)
                      .add_query("replica", "gcp"))
            resp = self.app.get(url, headers=get_auth_header())
            assert (200 == resp.status_code) or (206 == resp.status_code)

        check_dest()

    # TODO: (bhannafi) integration test of large collection manifest

    def _stage_file(self, key, size=7):
        assert size < MULTIPART_THRESHOLD
        data = os.urandom(size)
        chunk_size = get_s3_multipart_chunk_size(size)
        with ChecksummingSink(write_chunk_size=chunk_size) as sink:
            sink.write(data)
            sums = sink.get_checksums()
        metadata = {
            'hca-dss-crc32c': sums['crc32c'].lower(),
            'hca-dss-s3_etag': sums['s3_etag'].lower(),
            'hca-dss-sha1': sums['sha1'].lower(),
            'hca-dss-sha256': sums['sha256'].lower(),
        }
        fh = io.BytesIO(data)
        blob = self.s3.Bucket(os.environ['DSS_S3_BUCKET_TEST']).Object(key)
        blob.upload_fileobj(fh, ExtraArgs=dict(Metadata=metadata, ContentType="application/octet-stream"))
        return f"s3://{os.environ['DSS_S3_BUCKET_TEST']}/{key}"

    def _put_files(self, source_url, replica="aws", number_of_files=10):
        def _put():
            file_uuid = str(uuid.uuid4())
            file_version = str(datetime.datetime.utcnow().strftime("%Y-%m-%dT%H%M%S.%fZ"))
            with self.flask_app.test_request_context('/'):
                files_api.put(file_uuid, dict(source_url=source_url, creator_uid=1), file_version)
            return file_uuid, file_version

        with mock.patch.object(files_api.security, "request"):
            with mock.patch.object(files_api.security, "assert_authorized_group"):
                with ThreadPoolExecutor(max_workers=20) as e:
                    futures = [e.submit(_put) for _ in range(number_of_files)]
                    return [future.result() for future in futures]


if __name__ == '__main__':
    unittest.main()
