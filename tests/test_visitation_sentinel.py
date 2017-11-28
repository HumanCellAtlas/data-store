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
from dss.stepfunctions import visitation

sys.path.insert(0, os.path.join(
    pkg_root,
    'daemons',
    'dss-visitation-sentinel'
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


class TestVisitationSentinel(unittest.TestCase):

    def setUp(self):

        sys.path.insert(0, os.path.join(pkg_root, 'daemons', 'dss-visitation-sentinel'))  # noqa

        self.s3_test_bucket = get_env("DSS_S3_BUCKET_TEST")

        self.event = {
            "visitation_class_name": "IntegrationTest",
            "replica": "aws",
            "bucket": "bhannafi-dss-test",
            "k_workers": 3,
            "waiting": ['aa', '11', 'bc'],
            "name": uuid4()
        }

    def test_initialize_pass(self):

        app.initialize(self.event, context=None)

    def test_initialize_fail(self):

        self.event['bucket'] = str(uuid4())

        with self.assertRaises(ClientError):
            app.initialize(self.event, context=None)

    def test_muster(self):

        class VisTest(visitation.Visitation):
            @classmethod
            def get_status(cls, name):
                return {
                    'running': [],
                    'succeeded': [],
                    'failed': [],
                    'k_api_calls': 3
                }

            def start_walker(self, pfx):
                pass

        sentinel = VisTest.sentinel_state(
            self.event
        )

        running = sentinel.muster()

        self.assertEquals(
            set(self.event['waiting']),
            set(running)
        )

    def test_start_walker(self):

        class VisTest(visitation.Visitation):
            @classmethod
            def get_status(cls, name):
                return {
                    'running': [],
                    'succeeded': [],
                    'failed': [],
                    'k_api_calls': 3
                }

        sentinel = VisTest.sentinel_state(
            self.event
        )

        sentinel.start_walker(
            'aa'
        )


if __name__ == '__main__':
    unittest.main()
