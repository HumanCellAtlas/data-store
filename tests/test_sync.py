#!/usr/bin/env python
# coding: utf-8

from __future__ import (
    absolute_import,
    division,
    print_function,
    unicode_literals,
)

import os
import sys
import logging
import unittest

import boto3
import google.cloud.storage

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, pkg_root)

from dss.events.handlers import sync # noqa
from tests import infra  # noqa

infra.start_verbose_logging()


class TestSyncUtils(unittest.TestCase):
    def test_sync_blob(self):
        gcs_bucket_name, s3_bucket_name = (os.environ["DSS_GCS_TEST_BUCKET"],
                                           os.environ["DSS_S3_TEST_BUCKET"])
        logger = logging.getLogger(__name__)
        s3 = boto3.resource("s3")
        payload = os.urandom(2**20)
        test_key = "hca-dss-s3-to-gcs-sync-test"
        s3.Bucket(s3_bucket_name).Object(test_key).put(Body=payload)
        sync.sync_blob(
            source_platform="s3",
            source_key=test_key,
            dest_platform="gcs",
            logger=logger)
        # TODO: wait for GCSTS job and read back key

        test_key = "hca-dss-gcs-to-s3-sync-test"
        gcs_key_file = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
        gcs = google.cloud.storage.Client.from_service_account_json(
            gcs_key_file)
        gcs.bucket(gcs_bucket_name).blob(test_key).upload_from_string(payload)
        sync.sync_blob(
            source_platform="gcs",
            source_key=test_key,
            dest_platform="s3",
            logger=logger)
        dest_blob = s3.Bucket(s3_bucket_name).Object(test_key)
        self.assertEqual(dest_blob.get()["Body"].read(), payload)

if __name__ == '__main__':
    unittest.main()
