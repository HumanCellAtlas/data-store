#!/usr/bin/env python3
import io
import os
import sys
import json
import unittest
import logging
import typing
import boto3
import time
import importlib
from unittest import mock
from uuid import uuid4
from datetime import datetime
from datetime import timedelta
from requests.utils import parse_header_links

from flashflood import replay_event_stream
import requests

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss.storage.identifiers import TOMBSTONE_SUFFIX
from dss.util.aws import resources
from dss import events
from dss.config import BucketConfig, override_bucket_config, Config, Replica
from dss.util.version import datetime_to_version_format
from dss.util import UrlBuilder
from dss.util.version import datetime_from_timestamp
from tests.infra import DSSAssertMixin, testmode
from tests.infra.server import ThreadedLocalServer, MockFusilladeHandler
from tests import get_auth_header
import tests
daemon_app = importlib.import_module('daemons.dss-events-scribe.app')


logger = logging.getLogger(__name__)


def setUpModule():
    Config.set_config(BucketConfig.TEST)
    MockFusilladeHandler.start_serving()


def tearDownModule():
    MockFusilladeHandler.stop_serving()


class TestEventsUtils(unittest.TestCase, DSSAssertMixin):
    def test_build_bundle_metadata_document(self):
        dss.Config.set_config(dss.BucketConfig.TEST)
        app = ThreadedLocalServer()
        app.start()
        self.addCleanup(app.shutdown)
        for replica in Replica:
            uuid, version = _upload_bundle(app, replica)
            key = f"bundles/{uuid}.{version}"
            with self.subTest("Build normal bundle metadata document", replica=replica):
                md = events.build_bundle_metadata_document(replica, key)
                self.assertIn("manifest", md)
                self.assertIn("files", md)
                self.assertIn("version", md['manifest'])
                self.assertIn("files", md['manifest'])
                self.assertEqual(md['event_type'], "CREATE")
                self.assertEqual(md['bundle_info']['uuid'], uuid)
                self.assertEqual(md['bundle_info']['version'], version)
            _tombstone_bundle(app, replica, uuid, version)
            with self.subTest("Build tombstoned bundle metadata document", replica=replica):
                md = events.build_bundle_metadata_document(replica, f"{key}.{TOMBSTONE_SUFFIX}")
                self.assertNotIn("manifest", md)
                self.assertEqual(md['event_type'], "TOMBSTONE")
                self.assertEqual(md['bundle_info']['uuid'], uuid)
                self.assertEqual(md['bundle_info']['version'], version)

    def test_get_deleted_bundle_metadata_document(self):
        uuid = "f3cafb77-84ea-4050-98c9-2e3935f90f16"
        version = "2019-11-15T183956.169809Z"
        md = events.get_deleted_bundle_metadata_document("", f"bundles/{uuid}.{version}")
        self.assertEqual(md['event_type'], "DELETE")
        self.assertEqual(md['bundle_info']['uuid'], uuid)
        self.assertEqual(md['bundle_info']['version'], version)

    def test_record_event_for_bundle(self):
        metadata_document = dict(foo=f"{uuid4()}")
        key = f"bundles/{uuid4()}.{datetime_to_version_format(datetime.utcnow())}"
        test_parameters = [(replica, pfxs) for replica in Replica for pfxs in [None, ("foo", "bar")]]
        for replica, prefixes in test_parameters:
            with self.subTest(replica=replica.name, flashflood_prefixes=prefixes):
                self._test_record_event_for_bundle(replica, prefixes, metadata_document, key)

    def _test_record_event_for_bundle(self, replica, prefixes, metadata_document, key):
        with mock.patch("dss.events.build_bundle_metadata_document", return_value=metadata_document):
            ff = mock.MagicMock()
            ff.event_exists = mock.MagicMock(return_value=False)
            with mock.patch("dss.events.Config.get_flashflood_handle", return_value=ff):
                ret = events.record_event_for_bundle(replica, key, prefixes)
                used_prefixes = prefixes or replica.flashflood_prefix_write
                self.assertEqual(len(used_prefixes), ff.put.call_count)
                self.assertEqual(metadata_document, ret)
                for args, pfx in zip(ff.call_args_list, used_prefixes):
                    expected = ((resources.s3, Config.get_flashflood_bucket(), pfx),)
                    self.assertEqual(args, expected)

    def test_delete_event_for_bundle(self):
        key = f"bundles/{uuid4()}.{datetime_to_version_format(datetime.utcnow())}"
        test_parameters = [(replica, pfxs) for replica in Replica for pfxs in [None, ("foo", "bar")]]
        for replica, prefixes in test_parameters:
            with self.subTest(replica=replica.name, flashflood_prefixes=prefixes):
                self._test_delete_event_for_bundle(replica, prefixes, key)

    def _test_delete_event_for_bundle(self, replica, prefixes, key):
        ff = mock.MagicMock()
        with mock.patch("dss.events.Config.get_flashflood_handle", return_value=ff):
            events.delete_event_for_bundle(replica, key, prefixes)
            used_prefixes = prefixes or replica.flashflood_prefix_write
            self.assertEqual(len(used_prefixes), ff.delete_event.call_count)
            for args, pfx in zip(ff.call_args_list, used_prefixes):
                expected = ((resources.s3, Config.get_flashflood_bucket(), pfx),)
                self.assertEqual(args, expected)

    def test_journal_flashflood(self):
        number_of_events = 5
        ff = mock.MagicMock()
        with mock.patch("dss.events.Config.get_flashflood_handle", return_value=ff):
            with mock.patch("dss.events.list_new_flashflood_journals", return_value=range(17)):
                events.journal_flashflood("pfx", number_of_events)
                name, args, kwargs = ff.mock_calls[-1]
                self.assertEqual("combine_journals", name)

    # TODO: Add test for dss.events.list_new_flashflood_journals

    def test_update_flashflood(self):
        number_of_updates_to_apply = 3
        ff = mock.MagicMock()
        with mock.patch("dss.config.Config.get_flashflood_handle", return_value=ff):
            events.update_flashflood("pfx", number_of_updates_to_apply)
            name, args, kwargs = ff.mock_calls[0]
            self.assertEqual(number_of_updates_to_apply, args[0])

class TestEvents(unittest.TestCase, DSSAssertMixin):
    @classmethod
    def setUpClass(cls):
        cls.app = ThreadedLocalServer()
        cls.app.start()
        cls.bundles = {replica.name: list() for replica in Replica}
        with override_bucket_config(BucketConfig.TEST):
            for replica in Replica:
                pfx = f"flashflood-{replica.name}-{uuid4()}"
                os.environ[f'DSS_{replica.name.upper()}_FLASHFLOOD_PREFIX_READ'] = pfx
                os.environ[f'DSS_{replica.name.upper()}_FLASHFLOOD_PREFIX_WRITE'] = pfx
                for _ in range(3):
                    uuid, version = _upload_bundle(cls.app, replica)
                    cls.bundles[replica.name].append((uuid, version))
                    events.record_event_for_bundle(replica,
                                                   f"bundles/{uuid}.{version}",
                                                   use_version_for_timestamp=True)

    def setUp(self):
        dss.Config.set_config(dss.BucketConfig.TEST)

    @classmethod
    def teardownClass(cls):
        cls.app.shutdown()

    def test_get_event(self):
        for replica in Replica:
            uuid, version = self.bundles[replica.name][0]
            url = (UrlBuilder()
                   .set(path="/v1/events/" + uuid)
                   .add_query("replica", replica.name)
                   .add_query("version", version))
            with self.subTest("event found", replica=replica.name):
                res = self.app.get(str(url))
                md = events.build_bundle_metadata_document(replica, f"bundles/{uuid}.{version}")
                self.assertEqual(md, res.json())
            with self.subTest("event not found returns 404", replica=replica.name):
                url.set(path=f"/v1/events/{uuid4()}")
                res = self.app.get(str(url))
                self.assertEqual(res.status_code, requests.codes.not_found)

    def test_list_events(self):
        for replica in Replica:
            expected_docs = [events.build_bundle_metadata_document(replica, f"bundles/{uuid}.{version}")
                             for uuid, version in self.bundles[replica.name]]

            from_date = to_date = None
            with self.subTest("Omitting date range should list all events", replica=replica.name):
                self._test_list_events(replica, from_date, to_date, expected_docs, {200, 206})

            from_date = self.bundles[replica.name][0][1]
            to_date = None
            with self.subTest("Should inclusively list from `from_date`", replica=replica.name):
                self._test_list_events(replica, from_date, to_date, expected_docs, {200, 206})

            from_date = None
            to_date = self.bundles[replica.name][-1][1]
            with self.subTest("Should exclusively list to `to_date`", replica=replica.name):
                self._test_list_events(replica, from_date, to_date, expected_docs[:-1], {200, 206})

            with self.subTest("Restricted dates should list single event", replica=replica.name):
                from_date = self.bundles[replica.name][0][1]
                to_date = self.bundles[replica.name][1][1]
                self._test_list_events(replica, from_date, to_date, expected_docs[:1], {200})
                from_date = self.bundles[replica.name][1][1]
                to_date = self.bundles[replica.name][2][1]
                self._test_list_events(replica, from_date, to_date, expected_docs[1:2], {200})

    def _test_list_events(self, replica, from_date, to_date, expected_docs, expected_codes):
        url = (UrlBuilder()
               .set(path="/v1/events")
               .add_query("replica", replica.name))
        if from_date is not None:
            url.add_query("from_date", from_date)
        if to_date is not None:
            url.add_query("to_date", to_date)
        event_streams = self._get_paged_response(str(url), expected_codes)
        self.assertEqual(len(expected_docs), len(event_streams))
        docs = [json.loads(event.data)
                for event_stream in event_streams
                for event in replay_event_stream(event_stream)]
        for doc, expected_doc in zip(docs, expected_docs):
            self.assertEqual(doc, expected_doc)

    def _get_paged_response(self, url, expected_codes={200, 206}):
        results = list()
        while url:
            res = self.app.get("/v1" + url.split("v1", 1)[1])
            self.assertIn(res.status_code, expected_codes)
            content_key = res.headers.get("X-OpenAPI-Paginated-Content-Key", "results")
            results.extend([result for result in res.json()[content_key]])
            url = res.links.get("next", {}).get("url")
        return results

class TestEventsDaemon(unittest.TestCase, DSSAssertMixin):
    def test_flashflood_journal_and_update(self):
        journal_flashflood_returns = {r.flashflood_prefix_read: [True, False] for r in Replica}
        update_flashflood_returns = {r.flashflood_prefix_read: [1, 0] for r in Replica}

        def mock_journal_flashflood(pfx, minimum_number_of_events):
            did_journal = journal_flashflood_returns[pfx].pop(0)
            return did_journal

        def mock_update_flashflood(pfx, number_of_updates_to_apply):
            number_of_updates_applied = update_flashflood_returns[pfx].pop(0)
            return number_of_updates_applied

        daemon_app.journal_flashflood = mock_journal_flashflood
        daemon_app.update_flashflood = mock_update_flashflood

        class Context:
            def get_remaining_time_in_millis(self):
                return 300 * 1000

        daemon_app.flashflood_journal_and_update({}, Context())
        for pfx in journal_flashflood_returns:
            self.assertEqual(0, len(journal_flashflood_returns[pfx]))
        for pfx in update_flashflood_returns:
            self.assertEqual(0, len(update_flashflood_returns[pfx]))

    def test_flashflood_journal_and_update_timeout(self):
        class Context:
            def get_remaining_time_in_millis(self):
                return 0.0

        with mock.patch("daemons.dss-events-scribe.app.journal_flashflood", side_effect=Exception()):
            with mock.patch("daemons.dss-events-scribe.app.update_flashflood", side_effect=Exception()):
                # This should timeout and not call journal_flashflood or update_flashflood
                daemon_app.flashflood_journal_and_update({}, Context())

def _upload_bundle(app, replica, uuid=None):
    files = list()
    test_fixtures_bucket = os.environ['DSS_GS_BUCKET_TEST_FIXTURES']
    for i in range(2):
        file_name = f"file_{i}"
        file_uuid, file_version = str(uuid4()), datetime_to_version_format(datetime.utcnow())
        source_url = f"{replica.storage_schema}://{test_fixtures_bucket}/test_good_source_data/0"
        resp = app.put(f"/v1/files/{file_uuid}?version={file_version}",
                       headers={** get_auth_header(), ** {'Content-Type': "application/json"}},
                       json=dict(creator_uid=0, source_url=source_url))
        resp.raise_for_status()
        files.append((file_uuid, file_version, file_name))
    bundle_uuid, bundle_version = str(uuid4()), datetime_to_version_format(datetime.utcnow())
    json_request_body = dict(creator_uid=0,
                             files=[dict(uuid=file_uuid, version=file_version, name=file_name, indexed=False)
                                    for file_uuid, file_version, file_name in files])
    resp = app.put(f"/v1/bundles/{bundle_uuid}?replica={replica.name}&version={bundle_version}",
                   headers={** get_auth_header(), ** {'Content-Type': "application/json"}},
                   json=json_request_body)
    resp.raise_for_status()
    resp = app.get(f"/v1/bundles/{bundle_uuid}?replica={replica.name}&version={bundle_version}")
    return bundle_uuid, bundle_version

def _tombstone_bundle(app, replica, uuid, version=None):
    url = f"/v1/bundles/{uuid}?replica={replica.name}"
    if version is not None:
        url += f"&version={version}"
    resp = app.delete(url,
                      headers={** get_auth_header(), ** {'Content-Type': "application/json"}},
                      json=dict(reason="testing"))
    resp.raise_for_status()

if __name__ == '__main__':
    unittest.main()
