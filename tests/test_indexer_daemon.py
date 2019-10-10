#!/usr/bin/env python
# coding: utf-8
import os
import sys
import json
from uuid import uuid4
import unittest
from unittest import mock
import importlib
from datetime import datetime

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.util.version import datetime_to_version_format
from tests.infra import testmode

daemon_app = importlib.import_module('daemons.dss-index.app')


class TestIndexerDaemon(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    @testmode.standalone
    def test_launch_from_operator_queue(self):
        key = f"bundles/{uuid4()}.{datetime_to_version_format(datetime.utcnow())}"
        tests = [(True, "process_new_indexable_object"),
                 (False, "index_object"),
                 (None, "index_object")]
        for send_notifications, expected_call in tests:
            if send_notifications is not None:
                msg = dict(replica="aws", key=key, send_notifications=send_notifications)
            else:
                msg = dict(replica="aws", key=key)
            event = dict(body=json.dumps(msg))
            with mock.patch("daemons.dss-index.app.Indexer") as indexer:
                daemon_app.launch_from_operator_queue(dict(Records=[event]), {})
                name, args, kwargs = indexer.mock_calls[-1]
                self.assertIn(expected_call, name)

if __name__ == '__main__':
    unittest.main()
