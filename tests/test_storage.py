#!/usr/bin/env python
# coding: utf-8

import os
import sys
import string
import unittest
from uuid import uuid4
from unittest import mock
from random import random, randint
from datetime import datetime, timedelta

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss import Replica
from dss.util.version import datetime_to_version_format
from dss.storage.identifiers import UUID_REGEX, TOMBSTONE_SUFFIX
from dss.storage.bundles import enumerate_available_bundles, get_tombstoned_bundles
from dss.logging import configure_test_logging
from tests.infra import testmode, MockStorageHandler


class MockCloudBlobstoreHandle:
    bundle_uuid: str = None
    tombstoned_bundles: list = None
    untombstoned_bundles: list = None
    tombstones: list = None
    listing: list = None

    @classmethod
    def list(cls, bucket, pfx):
        for fqid in cls.listing:
            yield fqid

    @classmethod
    def gen_bundle_listing(cls,
                           number_of_versions: int,
                           versioned_tombstone_probability: float=0.0,
                           unversioned_tombstone_probability: float=0.0):
        cls.bundle_uuid = str(uuid4())
        untombstoned_bundles = list()
        tombstoned_bundles = list()
        tombstones = list()
        for _ in range(number_of_versions):
            random_date = datetime.utcnow() - timedelta(days=randint(0, 364),
                                                        hours=randint(0, 23),
                                                        minutes=randint(0, 59))
            bundle_fqid = f"{cls.bundle_uuid}.{datetime_to_version_format(random_date)}"
            bundle_key = f"bundles/{bundle_fqid}"
            if random() <= versioned_tombstone_probability:
                tombstones.append(f"{bundle_key}.{TOMBSTONE_SUFFIX}")
                tombstoned_bundles.append(bundle_key)
            else:
                untombstoned_bundles.append(bundle_key)
        cls.tombstoned_bundles = tombstoned_bundles
        cls.untombstoned_bundles = untombstoned_bundles
        cls.tombstones = tombstones
        listing = untombstoned_bundles + tombstoned_bundles + tombstones
        if random() <= unversioned_tombstone_probability:
            listing.append(f"bundles/{cls.bundle_uuid}.{TOMBSTONE_SUFFIX}")
        cls.listing = sorted(listing)


def setUpModule():
    configure_test_logging()


@testmode.standalone
class TestRegexIdentifiers(unittest.TestCase):
    def test_REGEX_MATCHING(self):
        chars = string.ascii_lowercase + string.digits
        for i, c in enumerate(chars):
            uuid = f'{c*8}-{c*4}-{c*4}-{c*4}-{c*12}'
            self.assertTrue(UUID_REGEX.match(uuid), uuid)

        for i in range(100):
            uuid = str(uuid4())
            self.assertTrue(UUID_REGEX.match(uuid), uuid)


@testmode.standalone
class TestStorageBundles(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        dss.Config.set_config(dss.BucketConfig.TEST)

    @mock.patch("dss.Config.get_blobstore_handle")
    def test_uuid_enumeration(self, mock_list_v2):
        mock_list_v2.return_value = MockStorageHandler()
        resp = enumerate_available_bundles(replica='aws')
        for x in resp['bundles']:
            self.assertNotIn('.'.join([x['uuid'], x['version']]), MockStorageHandler.dead_bundles)
            self.assertNotIn('.'.join([x['uuid'], x['version']]), MockStorageHandler.dead_bundles_without_suffix)

    @mock.patch("dss.Config.get_blobstore_handle")
    def test_tombstone_pages(self, mock_list_v2):

        mock_list_v2.return_value = MockStorageHandler()
        for tests in MockStorageHandler.test_per_page:
            test_size = tests['size']
            last_good_bundle = tests['last_good_bundle']
            resp = enumerate_available_bundles(replica='aws', per_page=test_size)
            page_one = resp['bundles']
            for x in resp['bundles']:
                self.assertNotIn('.'.join([x['uuid'], x['version']]), MockStorageHandler.dead_bundles)
                self.assertNotIn('.'.join([x['uuid'], x['version']]), MockStorageHandler.dead_bundles_without_suffix)
            self.assertDictEqual(last_good_bundle, resp['bundles'][-1])
            search_after = resp['search_after']
            resp = enumerate_available_bundles(replica='aws', per_page=test_size,
                                               search_after=search_after)
            for x in resp['bundles']:
                self.assertNotIn('.'.join([x['uuid'], x['version']]), MockStorageHandler.dead_bundles)
                self.assertNotIn('.'.join([x['uuid'], x['version']]), MockStorageHandler.dead_bundles_without_suffix)
                self.assertNotIn(x, page_one)
    # TODO add test to enumerate list and ensure all bundles that should be present are there.
    # TODO: Add test for dss.storage.bundles.get_bundle_manifest
    # TODO: Add test for dss.storage.bundles.save_bundle_manifest

    @mock.patch("dss.storage.bundles.Config.get_blobstore_handle", return_value=MockCloudBlobstoreHandle)
    def test_get_tombstoned_bundles(self, _):
        with self.subTest("Retrieve bundle fqid associated with versioned tombstone"):
            mock_handle = MockCloudBlobstoreHandle
            mock_handle.gen_bundle_listing(1, versioned_tombstone_probability=1.0)
            for e in get_tombstoned_bundles(Replica.aws, mock_handle.tombstones[-1]):
                self.assertEqual(mock_handle.tombstoned_bundles[0], e)

        with self.subTest("Retrieve bundle fqids associated with unversioned tombstone"):
            mock_handle.gen_bundle_listing(10,
                                           versioned_tombstone_probability=0.5,
                                           unversioned_tombstone_probability=1.0)
            unversioned_tombstone_key = f"bundles/{mock_handle.bundle_uuid}.{TOMBSTONE_SUFFIX}"
            listed_keys = {e for e in get_tombstoned_bundles(Replica.aws, unversioned_tombstone_key)}
            expected_keys = {e for e in mock_handle.untombstoned_bundles}
            unexpected_keys = {e for e in mock_handle.tombstoned_bundles}
            self.assertEqual(listed_keys, expected_keys)
            self.assertNotIn(unversioned_tombstone_key, listed_keys)
            self.assertEqual(0, len(unexpected_keys.intersection(listed_keys)))

        with self.subTest("Passing in non-tombstone key should raise"):
            mock_handle.gen_bundle_listing(1, versioned_tombstone_probability=1.0)
            with self.assertRaises(ValueError):
                for e in get_tombstoned_bundles(Replica.aws, "asdf"):
                    pass


if __name__ == '__main__':
    unittest.main()
