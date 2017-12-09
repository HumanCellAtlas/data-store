#!/usr/bin/env python
# coding: utf-8

import base64
import datetime
import time
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
from dss import BucketConfig, Config
from dss.stepfunctions.visitation import Visitation
from dss.stepfunctions import step_functions_describe_execution
from dss.stepfunctions.visitation.integration_test import IntegrationTest

from tests import infra  # noqa
from tests.infra import get_env, testmode  # noqa


infra.start_verbose_logging()
logger = logging.getLogger(__name__)
Config.set_config(BucketConfig.NORMAL)


class TestVisitationWalker(unittest.TestCase):
    @testmode.standalone
    def test_sentinel_muster(self):
        class VT(Visitation):
            @classmethod
            def get_status(cls, name):
                return {
                    'running': ['3']
                }

            def _start_walker(self, pfx):
                pass

        state = {
            'replica': 'aws',
            'bucket': 'thisisafakebucket',
            'k_workers': 3,
            'waiting': ['1', '2', '3', '4'],
            'name': uuid4()
        }

        sentinel = VT.with_sentinel_state(state, logger)
        running = sentinel.sentinel_muster()

        self.assertEquals(sentinel.k_workers, len(running))

    @testmode.standalone
    def test_walker_walk(self):
        self._test_walker_walk('aws', get_env('DSS_S3_BUCKET_TEST_FIXTURES'))
        self._test_walker_walk('gcp', get_env('DSS_GS_BUCKET_TEST_FIXTURES'))

    def _test_walker_walk(self, replica, bucket):
        state = {
            'replica': replica,
            'bucket': bucket,
            'prefix': 'testList/p'
        }

        items = []

        class VT(Visitation):
            def process_item(self, key):
                items.append(key)

        walker = VT.with_walker_state(state, logger)
        walker.walker_walk()

        self.assertEquals(10, len(items))

    @testmode.standalone
    def test_get_state(self):
        state = {
            'replica': 'aws',
            'bucket': 'thisisafakebucket',
            'k_workers': 3,
            'waiting': ['1', '2', '3', '4'],
            'name': uuid4()
        }
        v = Visitation.with_sentinel_state(state, logger)
        st = v.get_state()
        self.assertIn('visitation_class_name', st)

    @testmode.integration
    def test_z_integration(self):
        self._test_z_integration('aws', get_env('DSS_S3_BUCKET_TEST'))
        self._test_z_integration('gcp', get_env('DSS_GS_BUCKET_TEST'))

    def _test_z_integration(self, replica, bucket):
        k_workers = 10
        name = IntegrationTest.start(replica, bucket, k_workers)
        print()
        print(f'Running visitation integration test for replica={replica}, bucket={bucket}, workers={k_workers}')

        while True:
            resp = step_functions_describe_execution('dss-visitation-{stage}', name)

            if 'RUNNING' != resp['status']:
                break

            time.sleep(5)

        self.assertEquals('SUCCEEDED', resp['status'])

        output = json.loads(resp.get('output', '{}'))
        print(f'visited {output["number_of_keys_processed"]} files')


if __name__ == '__main__':
    unittest.main()
