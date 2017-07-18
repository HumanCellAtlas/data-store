#!/usr/bin/env python
# coding: utf-8

from __future__ import (
    absolute_import,
    division,
    print_function,
    unicode_literals,
)

import datetime
import logging
import os
import sys
import unittest
import uuid

import boto3
import google.cloud.storage

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..')) # noqa
sys.path.insert(0, pkg_root) # noqa

import dss
from dss.events.handlers import sync
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
        s3 = boto3.resource("s3")
        self.s3_bucket = s3.Bucket(self.s3_bucket_name)

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
        sync.sync_blob(source_platform="s3", source_key=test_key, dest_platform="gs", logger=self.logger)
        self.assertEqual(gs_dest_blob.download_as_string(), payload)

        test_key = "hca-dss-sync-test/gcs-to-s3/{}".format(uuid.uuid4())
        src_blob = self.gs_bucket.blob(test_key)
        dest_blob = self.s3_bucket.Object(test_key)
        src_blob.metadata = test_metadata
        src_blob.upload_from_string(payload)
        sync.sync_blob(source_platform="gs", source_key=test_key, dest_platform="s3", logger=self.logger)
        self.assertEqual(dest_blob.get()["Body"].read(), payload)
        self.assertEqual(dest_blob.metadata, test_metadata)

        # GS metadata seems to take a while to propagate, so we wait until the above test completes to read it back
        gs_dest_blob.reload()
        self.assertEqual(gs_dest_blob.metadata, test_metadata)


if __name__ == '__main__':
    unittest.main()
