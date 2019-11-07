#!/usr/bin/env python
# coding: utf-8

import os
import sys
import string
import unittest
from uuid import uuid4
from unittest import mock

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss.storage.identifiers import UUID_REGEX
from dss.storage.bundles import enumerate_available_bundles
from dss.logging import configure_test_logging
from tests.infra import testmode, MockStorageHandler


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
            self.assertDictEqual(last_good_bundle, resp['bundles'][-1])
            search_after = resp['search_after']
            resp = enumerate_available_bundles(replica='aws', per_page=test_size,
                                               search_after=search_after)
            for x in resp['bundles']:
                self.assertNotIn('.'.join([x['uuid'], x['version']]), MockStorageHandler.dead_bundles)
                self.assertNotIn(x, page_one)
    # TODO add test to enumerate list and ensure all bundles that should be present are there.
    # TODO: Add test for dss.storage.bundles.get_bundle_manifest
    # TODO: Add test for dss.storage.bundles.save_bundle_manifest


if __name__ == '__main__':
    unittest.main()
