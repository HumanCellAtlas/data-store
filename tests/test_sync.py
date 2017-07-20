#!/usr/bin/env python
# coding: utf-8

import base64
import datetime
import io
import logging
import os
import sys
import json
import hashlib
import unittest
import uuid
from argparse import Namespace

import boto3
from botocore.vendored import requests
import crcmod
import google.cloud.storage

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss.events.handlers import sync
from dss.util.streaming import get_pool_manager, S3SigningChunker
from tests import infra

infra.start_verbose_logging()


class TestSyncUtils(unittest.TestCase):
    def setUp(self):
        dss.Config.set_config(dss.BucketStage.TEST)
        self.gs_bucket_name, self.s3_bucket_name = dss.Config.get_gs_bucket(), dss.Config.get_s3_bucket()
        self.logger = logging.getLogger(__name__)
        gcp_key_file = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
        gs = google.cloud.storage.Client.from_service_account_json(gcp_key_file)
        self.gs_bucket = gs.bucket(self.gs_bucket_name)
        self.s3 = boto3.resource("s3")
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)

    def cleanup_sync_test_objects(self, prefix="hca-dss-sync-test", age=datetime.timedelta(days=1)):
        for key in self.s3_bucket.objects.filter(Prefix=prefix):
            if key.last_modified < datetime.datetime.now(datetime.timezone.utc) - age:
                key.delete()
        for key in self.gs_bucket.list_blobs(prefix=prefix):
            if key.time_created < datetime.datetime.now(datetime.timezone.utc) - age:
                key.delete()

    def test_sync_blob(self):
        self.cleanup_sync_test_objects()
        payload = os.urandom(2**20)
        test_metadata = {"metadata-sync-test": str(uuid.uuid4())}
        test_key = "hca-dss-sync-test/s3-to-gcs/{}".format(uuid.uuid4())
        src_blob = self.s3_bucket.Object(test_key)
        gs_dest_blob = self.gs_bucket.blob(test_key)
        src_blob.put(Body=payload, Metadata=test_metadata)
        sync.sync_blob(source_platform="s3", source_key=test_key, dest_platform="gs", logger=self.logger, context=None)
        self.assertEqual(gs_dest_blob.download_as_string(), payload)

        test_key = "hca-dss-sync-test/gcs-to-s3/{}".format(uuid.uuid4())
        src_blob = self.gs_bucket.blob(test_key)
        dest_blob = self.s3_bucket.Object(test_key)
        src_blob.metadata = test_metadata
        src_blob.upload_from_string(payload)
        sync.sync_blob(source_platform="gs", source_key=test_key, dest_platform="s3", logger=self.logger, context=None)
        self.assertEqual(dest_blob.get()["Body"].read(), payload)
        self.assertEqual(dest_blob.metadata, test_metadata)

        # GS metadata seems to take a while to propagate, so we wait until the above test completes to read it back
        gs_dest_blob.reload()
        self.assertEqual(gs_dest_blob.metadata, test_metadata)

    def test_s3_streaming(self):
        boto3_session = boto3.session.Session()
        payload = io.BytesIO(os.urandom(2**20))
        test_key = "hca-dss-sync-test/s3-streaming-upload/{}".format(uuid.uuid4())
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
        test_key = "hca-dss-sync-test/compose-gs-blobs/{}".format(uuid.uuid4())
        blob_names = []
        total_payload = b""
        for part in range(3):
            payload = os.urandom(2**10)
            self.gs_bucket.blob(f"{test_key}.part{part}").upload_from_string(payload)
            blob_names.append(f"{test_key}.part{part}")
            total_payload += payload
        sync.compose_gs_blobs(self.gs_bucket, blob_names, test_key)
        self.assertEqual(self.gs_bucket.blob(test_key).download_as_string(), total_payload)
        for part in range(3):
            self.assertFalse(self.gs_bucket.blob(f"{test_key}.part{part}").exists())

    def test_copy_part_s3_to_gs(self):
        payload = os.urandom(2**20)
        test_key = "hca-dss-sync-test/copy-part/{}".format(uuid.uuid4())
        test_blob = self.s3_bucket.Object(test_key)
        test_blob.put(Body=payload)
        source_url = self.s3.meta.client.generate_presigned_url("get_object",
                                                                Params=dict(Bucket=self.s3_bucket.name, Key=test_key))
        part = dict(start=0, end=len(payload) - 1)
        upload_url = self.gs_bucket.blob(test_key).create_resumable_upload_session(size=len(payload))
        res = sync.copy_part(upload_url, source_url, dest_platform="gs", part=part, context=Namespace(log=logging.info))
        crc = crcmod.predefined.Crc('crc-32c')
        crc.update(payload)
        self.assertEqual(base64.b64decode(json.loads(res.content)["crc32c"]), crc.digest())

    def test_copy_part_gs_to_s3(self):
        payload = os.urandom(2**20)
        test_key = "hca-dss-sync-test/copy-part/{}".format(uuid.uuid4())
        test_blob = self.gs_bucket.blob(test_key)
        test_blob.upload_from_string(payload)
        source_url = test_blob.generate_signed_url(datetime.timedelta(hours=1))
        part = dict(start=0, end=2**20 - 1)
        upload_url = "{host}/{bucket}/{key}".format(host=self.s3.meta.client.meta.endpoint_url,
                                                    bucket=self.s3_bucket.name,
                                                    key=test_key)
        res = sync.copy_part(upload_url, source_url, dest_platform="s3", part=part, context=Namespace(log=logging.info))
        self.assertEqual(json.loads(res.headers["ETag"]), hashlib.md5(payload).hexdigest())

if __name__ == '__main__':
    unittest.main()
