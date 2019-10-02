#!/usr/bin/env python
# coding: utf-8
from bisect import bisect
import copy
import datetime
from hashlib import sha256
from itertools import chain
import json
import random
import subprocess
import time
import logging
import os
import sys
from types import SimpleNamespace
import unittest
import uuid

import boto3
from cloud_blobstore import PagedIter
from unittest import mock

from botocore.exceptions import ClientError

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.config import Config, BucketConfig, Replica
from dss.logging import configure_test_logging
from dss.index.es.backend import ElasticsearchIndexBackend
from dss.storage.identifiers import BundleFQID
from dss.util import require
from dss.util.time import SpecificRemainingTime
from dss.util.version import datetime_to_version_format
from dss.stepfunctions.visitation import Visitation
from dss.stepfunctions import step_functions_describe_execution
from dss.stepfunctions.visitation import implementation
from dss.stepfunctions.visitation.integration_test import IntegrationTest
from dss.stepfunctions.visitation import registered_visitations
from dss.stepfunctions.visitation.timeout import Timeout
from dss.stepfunctions.visitation import index
from dss.stepfunctions.visitation.storage import StorageVisitation

from tests import eventually
from tests.infra import get_env, testmode, MockLambdaContext
from tests.infra.server import MockFusilladeHandler


logger = logging.getLogger(__name__)


def setUpModule():
    configure_test_logging()
    print(" * * * * * mock fusillade server starting * * * * * ")
    MockFusilladeHandler.start_serving()
    print(" * * * * * mock fusillade server started - ok * * * * * ")


def tearDownModule():
    print(" * * * * * mock fusillade server stopping * * * * * ")
    MockFusilladeHandler.stop_serving()
    print(" * * * * * mock fusillade server stopped - ok * * * * * ")


class TestVisitationWalker(unittest.TestCase):
    def setUp(self):
        self.remaining_time = SpecificRemainingTime(10)
        Config.set_config(BucketConfig.TEST)
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

    @testmode.integration
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

        walker = VT._with_state(state, self.remaining_time)
        walker.walker_walk()

        self.assertEquals(10, len(items))

    @testmode.standalone
    def test_get_state(self):
        state = {
            'number_of_workers': 3,
            '_waiting_work_ids': ['1', '2', '3', '4'],
        }
        v = Visitation._with_state(state, self.remaining_time)
        st = v.get_state()
        self.assertIn('_visitation_class_name', st)

    @testmode.standalone
    def test_finalize(self):
        work_result = [1, 2]
        v = Visitation._with_state(dict(work_result=work_result), self.remaining_time)
        v.job_finalize()
        self.assertEquals(v.get_state()['work_result'], work_result)

    @testmode.integration
    def test_z_integration(self):
        self._test_z_integration('aws', self.s3_test_fixtures_bucket)
        self._test_z_integration('gcp', self.gs_test_fixtures_bucket)

    def _test_z_integration(self, replica, bucket):
        number_of_workers = 10
        visitation = IntegrationTest.start(number_of_workers, replica=replica, bucket=bucket)
        print()
        print(f'Visitation integration test replica={replica}, bucket={bucket}, number_of_workers={number_of_workers}')

        while True:
            try:
                resp = step_functions_describe_execution('dss-visitation-{stage}', visitation['name'])
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
                yield (self.start_after_key, {})

    def list_v2(self, *args, **kwargs):
        return self.Iterator()


def fake_get_blobstore_handle(replica):
    return FakeBlobStore()


def fake_bucket(self):
    return "no-bucket"


def fake_bundle_load(cls, bundle_fqid):
    if bundle_fqid.to_key() == FakeBlobStore.Iterator.keys[2]:
        time.sleep(2)
    return mock.MagicMock(lookup_tombstone=mock.MagicMock(return_value=None))


def fake_index_object(_self, key):
    if FakeBlobStore.Iterator.keys.index(key) % 2:
        raise Exception()


@testmode.standalone
class TestIndexVisitation(unittest.TestCase):

    @mock.patch('dss.Config.get_blobstore_handle', new=fake_get_blobstore_handle)
    @mock.patch('dss.Replica.bucket', new=fake_bucket)
    @mock.patch('dss.index.indexer.Indexer.index_object', new=fake_index_object)
    def test_walk(self):
        r = index.IndexVisitation._with_state(state={'replica': 'aws'},
                                              remaining_time=SpecificRemainingTime(300))
        r._walk()
        r.walker_finalize()
        self.assertEqual(r.work_result, dict(failed=5, indexed=5, processed=10))

    @mock.patch('dss.Config.get_blobstore_handle', new=fake_get_blobstore_handle)
    @mock.patch('dss.Replica.bucket', new=fake_bucket)
    @mock.patch('dss.index.bundle.Bundle.load', new=fake_bundle_load)
    @mock.patch('dss.index.es.backend.ElasticsearchIndexBackend.index_bundle')
    def test_timeout(self, index_bundle):
        timeout = ElasticsearchIndexBackend.timeout + index.IndexVisitation.shutdown_time
        r = index.IndexVisitation._with_state(state={'replica': 'aws'},
                                              remaining_time=SpecificRemainingTime(timeout + 1))
        # The third item will sleep for two seconds and that will push the time remaining to below the timeout
        r._walk()
        self.assertEquals(2, index_bundle.call_count)
        print(FakeBlobStore.Iterator.keys, r.marker)
        self.assertEquals(1, FakeBlobStore.Iterator.keys.index(r.marker))
        self.assertEquals('frank', r.token)

    @mock.patch('dss.Config.get_blobstore_handle', new=fake_get_blobstore_handle)
    @mock.patch('dss.Replica.bucket', new=fake_bucket)
    @mock.patch('dss.index.bundle.Bundle.load', new=fake_bundle_load)
    @mock.patch('dss.index.es.backend.ElasticsearchIndexBackend.index_bundle')
    def test_no_time_remaining(self, index_bundle):
        timeout = ElasticsearchIndexBackend.timeout + index.IndexVisitation.shutdown_time
        r = index.IndexVisitation._with_state(state={'replica': 'aws'},
                                              remaining_time=SpecificRemainingTime(timeout - 1))
        r._walk()
        self.assertEquals(0, index_bundle.call_count)
        self.assertIsNone(r.marker)
        self.assertIsNone(r.token)

    def test_job_initialize(self):
        for num_workers, num_work_ids in [(1, 16), (15, 16), (16, 16), (17, 256)]:
            with self.subTest(num_workers=num_workers, num_work_ids=num_work_ids):
                r = index.IndexVisitation._with_state(state={'replica': 'aws', '_number_of_workers': num_workers},
                                                      remaining_time=SpecificRemainingTime(300))
                r.job_initialize()
                self.assertEquals(num_work_ids, len(set(r.work_ids)))


@testmode.integration
class TestIntegration(unittest.TestCase):

    def test_storage_verification_bundles(self):
        self._test('storage', '--replica=aws', '--replica=gcp', '--prefix=423', 'verify', '--folder=bundles', '--quick')

    def test_storage_verification_blobs(self):
        self._test('storage', '--replica=aws', '--replica=gcp', '--prefix=423', 'verify', '--folder=blobs')

    def test_index_verification(self):
        self._test('index', '--replica=aws', '--prefix=42', 'verify')

    def _test(self, *args):
        command = (f'{pkg_root}/scripts/admin-cli.py',) + args
        logger.info('Running %r', command)
        input = subprocess.check_output(command)
        logger.info('Step function input: %s', input)
        input = json.loads(input.decode())
        arn = input['arn']
        output = self._get_execution_output(arn)
        logger.info('Step function output: %r', output)
        self.assertIn('work_result', output)

    @eventually(timeout=300, interval=10)
    def _get_execution_output(self, arn):
        sfns = boto3.client('stepfunctions')
        execution = sfns.describe_execution(executionArn=arn)
        status = execution['status']
        require(status in ('SUCCEEDED', 'RUNNING'), f'Unexpected execution status: {status}')
        self.assertIn('output', execution)
        return execution['output']


@testmode.standalone
class TestConsistencyVisitation(unittest.TestCase):
    # Number of keys returned per round-trip by the mock blobstore's list_v2 iterator
    page_size = 100
    # Number of bundles in each mock replica
    num_keys = 9 * page_size + random.randint(-page_size, page_size)
    replicas = [r for r in Replica]
    # Distance (in # of keys) between missing keys in each replica. Also inverse of the frequency of keys with
    # simulated errors in the name, metadata tags or content.
    key_drop_period = random.randint(10, 19)
    # How much time to give the walker for actually processing the bucket listings
    timeout = .1
    # Minimum number of walk() invocations per replica (assuming one worker)
    num_key_partitions = len(StorageVisitation.prefix_chars)

    # For blobs we'll only make the SHA-256 unique per key and assume constant values for the rest of the checksums
    sha1 = '9d6f8f4cf29695ac34dc3622f0f18799e9813cdd'
    s3_etag = '735c3985880aff4d54683fb34055a121'
    crc32c = 'AB12F7BB'

    def _setUp(self, folder):
        keys = (uuid.uuid4() for _ in range(self.num_keys))
        if folder == 'blobs':
            keys = (f"{sha256(key.bytes).hexdigest()}.{self.sha1}.{self.s3_etag}.{self.crc32c.lower()}" for key in keys)
        keys = sorted(f'{folder}/{key}' for key in keys)
        # Drop every n-th key from either replica, but don't drop a key from all replicas
        self.assertTrue(len(self.replicas) < self.key_drop_period)
        self.keys = {replica: [key for key in keys if hash(key) % self.key_drop_period != i]
                     for i, replica in enumerate(self.replicas)}
        self.blobstore_mocks = []

    @mock.patch('dss.util.aws.clients.stepfunctions.start_execution')
    @mock.patch('dss.config.Config.get_blobstore_handle')
    def test(self, get_blobstore_handle, start_execution):
        start_execution.side_effect = self._start_execution
        get_blobstore_handle.side_effect = self._get_blobstore_handle
        for quick in False, True:
            for folder in 'blobs', 'files', 'bundles':
                with self.subTest(quick=quick, folder=folder):
                    self._setUp(folder)
                    self._test(folder, quick)

    def _test(self, folder, quick):
        visitation = StorageVisitation.start(number_of_workers=1,
                                             replicas=[replica.name for replica in self.replicas],
                                             buckets=[None] * len(self.replicas),
                                             folder=folder,
                                             quick=quick)
        # Assert the magic value returned from _start_execution to prove that it was invoked
        num_missing_keys = int(visitation['arn'])
        self.assertEquals(len(self.replicas) * self.num_keys - sum(map(len, self.keys.values())), num_missing_keys)
        # The mock blob listing is slowed down artificially once per initial walk() invocation. This should trigger
        # one extra walk invocation per key partition and replica. Because other factors could also trigger a
        # timeout, we can only assert a lower bound.
        num_partitions = self.num_key_partitions * len(self.replicas)
        self.assertGreaterEqual(len(self.blobstore_mocks), 2 * num_partitions)
        # Each extra walk invocation should incur a resumption of the key iteration from a token (and usually a key
        # passed to the `start_after_key` argument unless the resumption occurs on the first key of the listing).
        calls = list(chain.from_iterable(mock.mock_calls for mock in self.blobstore_mocks))
        resumptions = sum(1 for name, args, kwargs in calls if name == 'list_v2' and kwargs['token'])
        self.assertEquals(resumptions, len(self.blobstore_mocks) - num_partitions)

    def _get_blobstore_handle(self, replica):
        keys = self.keys[replica]
        test = self

        class MockPagedIter(PagedIter):

            def __init__(self, *args, prefix=None, start_after_key=None, token=None):
                super().__init__()
                self.resumed = start_after_key is not None
                self.prefix = prefix
                self.token = token or prefix
                self.start_after_key = start_after_key

            def get_api_response(self, next_token):
                start = 0 if next_token is None else bisect(keys, next_token)
                stop = min(start + test.page_size, len(keys))
                return [key for key in keys[start:stop] if key.startswith(self.prefix)]

            def get_listing_from_response(self, resp):
                for i, key in enumerate(resp):
                    # Once per listing, wait a little to trigger the walker timeout
                    if not self.resumed and i == len(resp) // 2:
                        time.sleep(test.timeout * 1.25)
                    yield (key, {})

            def get_next_token_from_response(self, resp):
                return resp[-1] if resp else None

        def is_bad(key):
            return (hash(key) + 1) % self.key_drop_period == self.replicas.index(replica)

        def get_user_metadata(bucket, key):
            return {
                'hca-dss-content-type': 'application/octet-stream',
                'hca-dss-sha1': 'bad_sha1' if is_bad(key) else self.sha1,
                'hca-dss-s3_etag': self.s3_etag,
                'hca-dss-crc32c': self.crc32c,
                'hca-dss-sha256': key.split('/')[-1].split('.', 1)[0]
            } if key.startswith('blobs/') else {}

        def get_size(bucket, key):
            return 123

        def get_content_type(bucket, key):
            return 'application/octet-stream'

        def get_cloud_checksum(bucket, key):
            return 'bad_cloud_checksum' if is_bad(key) else (self.crc32c if replica == 'gcp' else self.s3_etag)

        def get(bucket, key):
            return 'bad_object' if is_bad(key) else key

        mock_blob_store = mock.MagicMock()
        self.blobstore_mocks.append(mock_blob_store)
        mock_blob_store.list_v2.side_effect = MockPagedIter
        mock_blob_store.get_user_metadata.side_effect = get_user_metadata
        mock_blob_store.get_size.side_effect = get_size
        mock_blob_store.get_content_type.side_effect = get_content_type
        mock_blob_store.get_cloud_checksum.side_effect = get_cloud_checksum
        return mock_blob_store

    def _start_execution(self, stateMachineArn, name, input):
        def context():
            return MockLambdaContext(StorageVisitation.shutdown_time + self.timeout)

        Config.set_config(BucketConfig.NORMAL)
        input = json.loads(input)
        state = implementation.job_initialize(input, context())
        walker_state = copy.deepcopy(state)
        state['work_result'] = []
        while walker_state['_status'] == 'init':
            walker_state = implementation.walker_initialize(walker_state, context(), 0)
            while walker_state['_status'] == 'walk':
                walker_state = implementation.walker_walk(walker_state, context(), 0)
            walker_state = implementation.walker_finalize(walker_state, context(), 0)
        self.assertEquals(walker_state['_status'], 'end')
        state['work_result'].append(walker_state['work_result'])
        state = implementation.job_finalize(state, context())
        work_result = state['work_result']
        # The final work result is one dict that maps the name of a statistic, like `missing`, to a list of integers,
        # one integer per replica. Transpose this into one dict per replica, disolving the value lists. Also splice
        #  in a special entry for the name of the replica.
        stat_names, value_lists = zip(*dict(replica=state['replicas'], **work_result).items())
        for replica_stats in zip(*value_lists):
            replica_stats = SimpleNamespace(**dict(zip(stat_names, replica_stats)))
            keys = self.keys[Replica[replica_stats.replica]]
            self.assertEquals(self.num_keys, replica_stats.missing + replica_stats.present)
            self.assertEquals(len(keys), replica_stats.present)
            if state['folder'] == 'blobs' and not state['quick']:
                self.assertGreater(replica_stats.bad_checksum, 0)
                self.assertGreater(replica_stats.bad_native_checksum, 0)
        work_result = SimpleNamespace(**work_result)
        if not state['quick']:
            # Inconsistencies between the replicas are counted for the minority replicas only. With two replicas
            # there is going to be a tie. One of them will get its count incremented, the other one will not. Even
            # though such an inconsistency occurs for multiple keys during the test, it might consistently be the
            # same replica that gets chosen as the minority. IOW, it is possible that all inconsistencies are counted
            # for one replica only.
            self.assertGreater(sum(work_result.inconsistent), 0)
        # Return the total number of missing keys (which is different between each test run because the overall number
        # of keys is random). The test can assert this value, proving that this was code actually run.
        return {'executionArn': str(sum(work_result.missing))}


if __name__ == '__main__':
    unittest.main()
