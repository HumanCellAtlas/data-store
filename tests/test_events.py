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
from uuid import uuid4
from datetime import datetime

from flashflood import replay_with_urls
import requests

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss import events
from dss.config import BucketConfig, Config, Replica, override_bucket_config
from dss.util.version import datetime_to_version_format
from dss.util import UrlBuilder
from tests.infra import DSSAssertMixin, testmode
from tests.infra.server import ThreadedLocalServer
from tests import get_auth_header
import tests


logger = logging.getLogger(__name__)


class TestEvents(unittest.TestCase, DSSAssertMixin):
    @classmethod
    def setUpClass(cls):
        cls.app = ThreadedLocalServer()
        cls.app.start()
        cls.bundle = dict()
        with override_bucket_config(dss.BucketConfig.TEST):
            for replica in Replica:
                bundle_uuid, bundle_version = cls._upload_bundle(replica)
                cls.bundle[replica.name] = dict(uuid=bundle_uuid,
                                                version=bundle_version,
                                                key=f"bundles/{bundle_uuid}.{bundle_version}",)

    @classmethod
    def teardownClass(cls):
        cls.app.shutdown()

    def setUp(self):
        Config.set_config(dss.BucketConfig.TEST)
        events.record_event_for_bundle.cache_clear()
        for replica in Replica:
            name = f'DSS_{replica.name.upper()}_FLASHFLOOD_PREFIX'
            os.environ[name] = f"flashflood-{uuid4()}"
            events.record_event_for_bundle(replica, self.bundle[replica.name]['key'])

    def test_list(self):
        for replica in Replica:
            self._test_list(replica)

    def _test_list(self, replica):
        res = self.app.get("/v1/events",
                           params=dict(replica=replica.name))
        self.assertIn(res.status_code, [requests.codes.ok, requests.codes.partial])
        event = [e for e in replay_with_urls(res.json())][0]
        event_doc = json.loads(event.data.decode("utf-8"))
        self.assertEqual(events._build_bundle_metadata_document(replica, self.bundle[replica.name]['key']), event_doc)

    def test_get(self):
        for replica in Replica:
            self._test_get(replica)

    def _test_get(self, replica):
        res = self.app.get("/v1/events/{}".format(self.bundle[replica.name]['uuid']),
                           params=dict(replica=replica.name, version=self.bundle[replica.name]['version']))
        self.assertEqual(events._build_bundle_metadata_document(replica, self.bundle[replica.name]['key']), res.json())

    @classmethod
    def _upload_bundle(cls, replica, uuid=None):
        files = list()
        test_fixtures_bucket = os.environ['DSS_GS_BUCKET_TEST_FIXTURES']
        for i in range(2):
            file_name = f"file_{i}"
            file_uuid, file_version = str(uuid4()), datetime_to_version_format(datetime.utcnow())
            source_url = f"{replica.storage_schema}://{test_fixtures_bucket}/test_good_source_data/0"
            resp = cls.app.put(f"/v1/files/{file_uuid}?version={file_version}",
                               headers={** get_auth_header(), ** {'Content-Type': "application/json"}},
                               json=dict(creator_uid=0, source_url=source_url))
            resp.raise_for_status()
            files.append((file_uuid, file_version, file_name))
        bundle_uuid, bundle_version = str(uuid4()), datetime_to_version_format(datetime.utcnow())
        json_request_body = dict(creator_uid=0,
                                 files=[dict(uuid=file_uuid, version=file_version, name=file_name, indexed=False)
                                        for file_uuid, file_version, file_name in files])
        resp = cls.app.put(f"/v1/bundles/{bundle_uuid}?replica={replica.name}&version={bundle_version}",
                           headers={** get_auth_header(), ** {'Content-Type': "application/json"}},
                           json=json_request_body)
        resp.raise_for_status()
        resp = cls.app.get(f"/v1/bundles/{bundle_uuid}?replica={replica.name}&version={bundle_version}")
        return bundle_uuid, bundle_version

if __name__ == '__main__':
    unittest.main()
