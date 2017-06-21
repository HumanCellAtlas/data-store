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
import time
import unittest
import uuid

import boto3
import google.cloud.storage

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, pkg_root)

from dss.events.handlers import sync # noqa
from tests import infra  # noqa

infra.start_verbose_logging()


class TestSyncUtils(unittest.TestCase):
    def setUp(self):
        self.gcs_bucket_name = os.environ["DSS_GCS_TEST_BUCKET"]
        self.s3_bucket_name = os.environ["DSS_S3_TEST_BUCKET"]
        self.logger = logging.getLogger(__name__)
        gcs_key_file = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
        gcs = google.cloud.storage.Client.from_service_account_json(gcs_key_file)
        self.gcs_bucket = gcs.bucket(self.gcs_bucket_name)
        s3 = boto3.resource("s3")
        self.s3_bucket = s3.Bucket(self.s3_bucket_name)

    def cleanup_sync_test_objects(self, prefix="hca-dss-sync-test", age=datetime.timedelta(days=1)):
        for key in self.s3_bucket.objects.filter(Prefix=prefix):
            if key.last_modified < datetime.datetime.now(datetime.timezone.utc) - age:
                key.delete()
        for key in self.gcs_bucket.list_blobs(prefix=prefix):
            if key.time_created < datetime.datetime.now(datetime.timezone.utc) - age:
                key.delete()

    def test_sync_blob(self):
        self.cleanup_sync_test_objects()
        payload, readback = os.urandom(2**20), b""
        test_key = "hca-dss-sync-test/s3-to-gcs/{}".format(uuid.uuid4())
        self.s3_bucket.Object(test_key).put(Body=payload)
        sync.sync_blob(source_platform="s3", source_key=test_key, dest_platform="gcs", logger=self.logger)
        if os.environ.get("DSS_RUN_LONG_TESTS"):
            for i in range(90):
                try:
                    readback = self.gcs_bucket.blob(test_key).download_as_string()
                    break
                except Exception as e:
                    time.sleep(5)
            self.assertEqual(readback, payload)

        test_key = "hca-dss-sync-test/gcs-to-s3/{}".format(uuid.uuid4())
        dest_blob = self.s3_bucket.Object(test_key)
        self.gcs_bucket.blob(test_key).upload_from_string(payload)
        sync.sync_blob(source_platform="gcs", source_key=test_key, dest_platform="s3", logger=self.logger)
        self.assertEqual(dest_blob.get()["Body"].read(), payload)


if __name__ == '__main__':
    unittest.main()
