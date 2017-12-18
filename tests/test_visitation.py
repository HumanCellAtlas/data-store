#!/usr/bin/env python
# coding: utf-8

import copy
import time
import logging
import os
import sys
import json
import unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss import BucketConfig, Config
from dss.stepfunctions.visitation import Visitation
from dss.stepfunctions import step_functions_describe_execution
from dss.stepfunctions.visitation import implimentation
from dss.stepfunctions.visitation.integration_test import IntegrationTest
from dss.stepfunctions.visitation import registered_visitations

from tests import infra  # noqa
from tests.infra import get_env, testmode  # noqa


infra.start_verbose_logging()
logger = logging.getLogger(__name__)


class TestVisitationWalker(unittest.TestCase):
    def setUp(self):
        dss.Config.set_config(dss.BucketConfig.TEST)
        self.s3_test_fixtures_bucket = get_env("DSS_S3_BUCKET_TEST_FIXTURES")
        self.gs_test_fixtures_bucket = get_env("DSS_GS_BUCKET_TEST_FIXTURES")
        self.s3_test_bucket = get_env("DSS_S3_BUCKET_TEST")
        self.gs_test_bucket = get_env("DSS_GS_BUCKET_TEST")

        class VT(Visitation):
            def walker_walk(self):
                pass

        registered_visitations.registered_visitations['VT'] = VT

        self.sentinel_state = {
            '_visitation_class_name': 'VT',
            'work_ids': ['1', '2', '3', '4'],
            '_number_of_workers': 3,
        }

        self.walker_state = {
            '_visitation_class_name': 'VT',
            '_processing_work_ids': ['1', '2', '3', '4'],
            'is_finished': False,
        }

    @testmode.standalone
    def test_implimentation_walker_initialize(self):
        state = copy.deepcopy(self.walker_state)
        state = implimentation.walker_initialize(state, None, (1,))
        self.assertEquals('2', state['work_id'])

        state = copy.deepcopy(self.walker_state)
        state = implimentation.walker_initialize(state, None, (0,))
        self.assertIsNotNone(state['_sentinel_state_copy'])

    @testmode.standalone
    def test_implimentation_walker_walk(self):
        implimentation.walker_walk(self.walker_state, None, (1,))

    @testmode.standalone
    def test_implimentation_walker_finalize(self):
        implimentation.walker_finalize(self.walker_state, None, (1,))

    @testmode.standalone
    def test_implimentation_walker_failed(self):
        implimentation.walker_failed(self.walker_state, None, (1,))

    @testmode.standalone
    def test_implimentation_sentinel_initialize(self):
        s = copy.deepcopy(self.sentinel_state)
        implimentation.sentinel_initialize(s, None)

    @testmode.standalone
    def test_imlimentation_muster(self):
        state = implimentation.sentinel_initialize(self.sentinel_state, None)
        state = implimentation.muster(state, None)
        self.assertEquals(len(state['_processing_work_ids']), state['_number_of_workers'])

    @testmode.standalone
    def test_imlimentation_sentinel_join(self):
        states = list()
        sent_state = copy.deepcopy(self.sentinel_state)
        sent_state['_processing_work_ids'] = sent_state['work_ids'][:sent_state['_number_of_workers']]
        sent_state['work_ids'] = sent_state['work_ids'][sent_state['_number_of_workers']:]
        for i in range(sent_state['_number_of_workers']):
            s = implimentation.walker_initialize(sent_state, None, (i,))
            states.append(s)
        state = implimentation.sentinel_join(states, None)
        self.assertFalse(state['is_finished'])

        states = list()
        sent_state = copy.deepcopy(self.sentinel_state)
        sent_state['_processing_work_ids'] = sent_state['work_ids'][:sent_state['_number_of_workers']]
        sent_state['work_ids'] = list()
        for i in range(3):
            s = implimentation.walker_initialize(sent_state, None, (i,))
            states.append(s)
        state = implimentation.sentinel_join(states, None)
        self.assertTrue(state['is_finished'])

    @testmode.standalone
    def test_implimentation_sentinel_finalize(self):
        implimentation.sentinel_finalize(self.sentinel_state, None)

    @testmode.standalone
    def test_implimentation_sentinel_failed(self):
        implimentation.sentinel_failed(self.sentinel_state, None)

    @testmode.standalone
    def test_integration_walk(self):
        self._test_integration_walk('aws', self.s3_test_fixtures_bucket)
        self._test_integration_walk('gcp', self.gs_test_fixtures_bucket)

    def _test_integration_walk(self, replica, bucket):
        state = {
            'replica': replica,
            'bucket': bucket,
            'work_id': 'testList/p',
        }

        items = []

        class VT(IntegrationTest):
            def process_item(self, key):
                items.append(key)

        walker = VT._with_walker_state(state, logger)
        walker.walker_walk()

        self.assertEquals(10, len(items))

    @testmode.standalone
    def test_get_state(self):
        state = {
            'number_of_workers': 3,
            '_waiting_work_ids': ['1', '2', '3', '4'],
        }
        v = Visitation._with_sentinel_state(state, logger)
        st = v.get_state()
        self.assertIn('_visitation_class_name', st)

    @testmode.integration
    def test_z_integration(self):
        self._test_z_integration('aws', self.s3_test_bucket)
        self._test_z_integration('gcp', self.gs_test_bucket)

    def _test_z_integration(self, replica, bucket):
        number_of_workers = 10
        name = IntegrationTest.start(replica, bucket, number_of_workers)
        print()
        print(f'Running visitation integration test for replica={replica}, bucket={bucket}, '
              f'number_of_workers={number_of_workers}')

        while True:
            resp = step_functions_describe_execution('dss-visitation-{stage}', name)

            if 'RUNNING' != resp['status']:
                break

            time.sleep(5)

        self.assertEquals('SUCCEEDED', resp['status'])


if __name__ == '__main__':
    unittest.main()
