#!/usr/bin/env python
# coding: utf-8

import copy
import datetime
import time
import logging
import os
import sys
import unittest
from unittest.mock import MagicMock
import uuid
import mock

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from botocore.exceptions import ClientError

import dss
from dss.logging import configure_test_logging
from dss.index.es.backend import ElasticsearchIndexBackend
from dss.storage.identifiers import BundleFQID
from dss.util.version import datetime_to_version_format
from dss.stepfunctions.visitation import Visitation
from dss.stepfunctions import step_functions_describe_execution
from dss.stepfunctions.visitation import implementation
from dss.stepfunctions.visitation.integration_test import IntegrationTest
from dss.stepfunctions.visitation import registered_visitations
from dss.stepfunctions.visitation.timeout import Timeout
from dss.stepfunctions.visitation import reindex

from tests.infra import get_env, testmode, MockLambdaContext

logger = logging.getLogger(__name__)


def setUpModule():
    configure_test_logging()


class TestVisitationWalker(unittest.TestCase):
    def setUp(self):
        self.context = MockLambdaContext()
        dss.Config.set_config(dss.BucketConfig.TEST)
        self.s3_test_fixtures_bucket = get_env("DSS_S3_BUCKET_TEST_FIXTURES")
        self.gs_test_fixtures_bucket = get_env("DSS_GS_BUCKET_TEST_FIXTURES")
        self.s3_test_bucket = get_env("DSS_S3_BUCKET_TEST")
        self.gs_test_bucket = get_env("DSS_GS_BUCKET_TEST")

        class VT(Visitation):
            def walker_walk(self):
                pass

        registered_visitations.registered_visitations['VT'] = VT

        self.job_state = {
            '_visitation_class_name': 'VT',
            'work_ids': ['1', '2', '3', '4'],
            '_number_of_workers': 3,
        }

        self.walker_state = {
            '_visitation_class_name': 'VT',
            'work_ids': [['1', '2'], ['3', '4']],
        }

    @testmode.standalone
    def test_implementation_walker_initialize(self):
        state = copy.deepcopy(self.walker_state)
        state = implementation.walker_initialize(state, None, 0)
        self.assertEquals('1', state['work_id'])

    @testmode.standalone
    def test_implementation_walker_walk(self):
        implementation.walker_walk(self.walker_state, None, 1)

    @testmode.standalone
    def test_implementation_walker_finalize(self):
        implementation.walker_finalize(self.walker_state, None, 1)

    @testmode.standalone
    def test_implementation_walker_failed(self):
        implementation.walker_failed(self.walker_state, None, 1)

    @testmode.standalone
    def test_implementation_job_initialize(self):
        s = copy.deepcopy(self.job_state)
        implementation.job_initialize(s, None)

    @testmode.standalone
    def test_implementation_job_finalize(self):
        implementation.job_finalize(self.job_state, None)

    @testmode.standalone
    def test_implementation_job_failed(self):
        implementation.job_failed(self.job_state, None)

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

        walker = VT._with_state(state, self.context)
        walker.walker_walk()

        self.assertEquals(10, len(items))

    @testmode.standalone
    def test_get_state(self):
        state = {
            'number_of_workers': 3,
            '_waiting_work_ids': ['1', '2', '3', '4'],
        }
        v = Visitation._with_state(state, self.context)
        st = v.get_state()
        self.assertIn('_visitation_class_name', st)

    @testmode.standalone
    def test_finalize(self):
        work_result = [1, 2]
        v = Visitation._with_state(dict(work_result=work_result), self.context)
        v.job_finalize()
        self.assertEquals(v.get_state()['work_result'], work_result)

    @testmode.integration
    def test_z_integration(self):
        self._test_z_integration('aws', self.s3_test_fixtures_bucket)
        self._test_z_integration('gcp', self.gs_test_fixtures_bucket)

    def _test_z_integration(self, replica, bucket):
        number_of_workers = 10
        name = IntegrationTest.start(replica, bucket, number_of_workers)
        print()
        print(f'Visitation integration test replica={replica}, bucket={bucket}, number_of_workers={number_of_workers}')

        while True:
            try:
                resp = step_functions_describe_execution('dss-visitation-{stage}', name)
                if 'RUNNING' != resp['status']:
                    break
            except ClientError as e:
                if e.response['Error']['Code'] == 'ExecutionDoesNotExist':
                    print("Execution has not started yet. Retrying soon...")

            time.sleep(5)

        self.assertEquals('SUCCEEDED', resp['status'])


@testmode.standalone
class TestTimeout(unittest.TestCase):

    def test_timeout_did(self):
        with Timeout(1) as timeout:
            time.sleep(2)
        self.assertTrue(timeout.did_timeout)

    def test_timeout_did_not(self):
        with Timeout(2) as timeout:
            pass
        self.assertFalse(timeout.did_timeout)

    def test_timeout_exception(self):
        class TestException(Exception):
            pass

        with self.assertRaises(TestException):
            with Timeout(1) as timeout:
                raise TestException()
        self.assertFalse(timeout.did_timeout)


class FakeBlobStore:
    class Iterator:
        keys = [BundleFQID(uuid=uuid.uuid4(),
                           version=datetime_to_version_format(datetime.datetime.utcnow())).to_key()
                for i in range(10)]

        def __init__(self, *args, **kwargs):
            self.start_after_key = None
            self.token = 'frank'

        def __iter__(self):
            for key in self.keys:
                self.start_after_key = key
                yield self.start_after_key

    def list_v2(self, *args, **kwargs):
        return self.Iterator()


def fake_get_blobstore_handle(replica):
    return FakeBlobStore()


def fake_bucket(self):
    return "no-bucket"


def fake_bundle_load(cls, bundle_fqid):
    if bundle_fqid.to_key() == FakeBlobStore.Iterator.keys[2]:
        time.sleep(2)
    return MagicMock(lookup_tombstone=MagicMock(return_value=None))


def fake_index_object(_self, key):
    if FakeBlobStore.Iterator.keys.index(key) % 2:
        raise Exception()


class TestVisitationReindex(unittest.TestCase):

    @testmode.standalone
    @mock.patch('dss.Config.get_blobstore_handle', new=fake_get_blobstore_handle)
    @mock.patch('dss.Replica.bucket', new=fake_bucket)
    @mock.patch('dss.index.indexer.Indexer.index_object', new=fake_index_object)
    def test_reindex_walk(self):
        r = reindex.Reindex._with_state(state={'replica': 'aws'},
                                        context=MockLambdaContext())
        r._walk()
        r.walker_finalize()
        self.assertEqual(r.work_result, dict(failed=5, indexed=5, processed=10))

    @testmode.standalone
    @mock.patch('dss.Config.get_blobstore_handle', new=fake_get_blobstore_handle)
    @mock.patch('dss.Replica.bucket', new=fake_bucket)
    @mock.patch('dss.index.bundle.Bundle.load', new=fake_bundle_load)
    @mock.patch('dss.index.es.backend.ElasticsearchIndexBackend.index_bundle')
    def test_reindex_timeout(self, index_bundle):
        r = reindex.Reindex._with_state(state={'replica': 'aws'},
                                        context=MockLambdaContext(timeout=ElasticsearchIndexBackend.timeout + 1))
        # The third item will sleep for two seconds and that will push the time remaining to below the timeout
        r._walk()
        self.assertEquals(2, index_bundle.call_count)
        print(FakeBlobStore.Iterator.keys, r.marker)
        self.assertEquals(1, FakeBlobStore.Iterator.keys.index(r.marker))
        self.assertEquals('frank', r.token)

    @testmode.standalone
    @mock.patch('dss.Config.get_blobstore_handle', new=fake_get_blobstore_handle)
    @mock.patch('dss.Replica.bucket', new=fake_bucket)
    @mock.patch('dss.index.bundle.Bundle.load', new=fake_bundle_load)
    @mock.patch('dss.index.es.backend.ElasticsearchIndexBackend.index_bundle')
    def test_reindex_no_time_remaining(self, index_bundle):
        r = reindex.Reindex._with_state(state={'replica': 'aws'},
                                        context=MockLambdaContext(timeout=ElasticsearchIndexBackend.timeout - 1))
        r._walk()
        self.assertEquals(0, index_bundle.call_count)
        self.assertIsNone(r.marker)
        self.assertIsNone(r.token)

    @testmode.standalone
    def test_job_initialize(self):
        for num_workers, num_work_ids in [(1, 16), (15, 16), (16, 16), (17, 256)]:
            with self.subTest(num_workers=num_workers, num_work_ids=num_work_ids):
                r = reindex.Reindex._with_state(state={'replica': 'aws', '_number_of_workers': num_workers},
                                                context=MockLambdaContext())
                r.job_initialize()
                self.assertEquals(num_work_ids, len(set(r.work_ids)))


if __name__ == '__main__':
    unittest.main()
