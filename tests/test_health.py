import os
import sys
import unittest
import json

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.api import health
from tests.infra import testmode


def _es_invalid():
    return False, {"es": "error"}


def _es_valid():
    return True, None


def _er_invalid():
    return False, {"er": "Error"}


def _er_valid():
    return True, None


def _dynamodb_invalid():
    return False, {"ddb": "error"}


def _dynamodb_valid():
    return True, None


class TestHealth(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    @testmode.standalone
    def test_elastic_search(self):
        test_es = json.loads(health.l2_health_checks(get_es_status=_es_valid))
        self.assertNotIn("elasticSearch", test_es.keys())

        test_es = json.loads(health.l2_health_checks(get_es_status=_es_invalid))
        self.assertIn("elasticSearch", test_es.keys())

    @testmode.standalone
    def test_dynamodb(self):
        test_ddb = json.loads(health.l2_health_checks(get_dynamodb_status=_dynamodb_valid))
        self.assertNotIn("dynamoDB", test_ddb.keys())

        test_ddb = json.loads(health.l2_health_checks(get_dynamodb_status=_dynamodb_invalid))
        self.assertIn("dynamoDB", test_ddb.keys())

    @testmode.standalone
    def test_event_relay(self):
        test_er = json.loads(health.l2_health_checks(get_er_status=_er_valid))
        self.assertNotIn("eventRelay", test_er.keys())

        test_er = json.loads(health.l2_health_checks(get_er_status=_er_invalid))
        self.assertIn("eventRelay", test_er.keys())

    @testmode.standalone
    def test_healthy(self):
        test_health = json.loads(health.l2_health_checks(get_es_status=_es_valid, get_dynamodb_status=_dynamodb_valid,
                                                         get_er_status=_er_valid))
        self.assertDictEqual({"Healthy": True}, test_health)


if __name__ == "__main__":
    unittest.main()
