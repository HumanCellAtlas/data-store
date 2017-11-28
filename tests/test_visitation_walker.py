#!/usr/bin/env python
# coding: utf-8

import base64
import datetime
import logging
import os
import sys
import json
import hashlib
import unittest
from uuid import uuid4
from argparse import Namespace

import boto3
import crcmod
import google.cloud.storage
from botocore.client import ClientError

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss

sys.path.insert(0, os.path.join(
    pkg_root,
    'daemons',
    'dss-visitation-walker'
))

import app # noqa
from tests import infra # noqa
from tests.infra import get_env # noqa


infra.start_verbose_logging()


"""
Test the visitation batch processor
"""


s3_test_bucket = None
def setUpModule():
    global s3_test_bucket
    s3_test_bucket = get_env("DSS_S3_BUCKET_TEST")


class TestVisitationWalker(unittest.TestCase):

    def setUp(self):

        sys.path.insert(0, os.path.join(pkg_root, 'daemons', 'dss-visitation-walker'))  # noqa

        self.s3_test_bucket = get_env("DSS_S3_BUCKET_TEST")

        self.event = {
            "visitation_class_name": "IntegrationTest",
            "replica": "aws",
            "bucket": self.s3_test_bucket,
            "prefix": "11",
        }

    def test_initialize_pass(self):

        app.initialize(self.event, context=None)

    def test_initialize_fail(self):

        self.event['bucket'] = str(uuid4())

        with self.assertRaises(ClientError):
            app.initialize(self.event, context=None)

    def test_walk(self):

        app.walk(
            self.event,
            None
        )


if __name__ == '__main__':
    unittest.main()
