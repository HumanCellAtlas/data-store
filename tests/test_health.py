import os
import sys
import unittest
import json

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.api import health
from tests.infra import testmode


class TestHealth(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    @testmode.standalone
    def test_elastic_search(self):
        test_es = health._get_es_status()
        self.assertIn(True, test_es)

    @testmode.standalone
    def test_dynamodb(self):
        test_ddb = health._get_dynamodb_status()
        self.assertIn(True, test_ddb)

    @testmode.standalone
    def test_event_relay(self):
        test_er = health._get_event_relay_status()
        self.assertIn(True, test_er)

    @testmode.standalone
    def test_healthy(self):
        test_health = json.loads(health.l2_health_checks())
        self.assertDictEqual({"Healthy": True}, test_health)


if __name__ == "__main__":
    unittest.main()
