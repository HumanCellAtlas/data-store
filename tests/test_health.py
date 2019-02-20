import os
import sys
import unittest
import json
import logging

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.api import health
from tests.infra import testmode
from tests.es import ElasticsearchServer

logger = logging.getLogger(__name__)


class TestHealth(unittest.TestCase):
    server = None

    @classmethod
    def setUpClass(cls):
        cls.server = ElasticsearchServer()
        os.environ['DSS_ES_PORT'] = str(cls.server.port)
        pass

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        os.unsetenv('DSS_ES_PORT')
        pass

    @testmode.standalone
    def test_elastic_search(self):
        test_es = health._get_es_status(port=os.getenv("DSS_ES_PORT"))
        self.assertIn(True, test_es)

    @testmode.standalone
    def test_dynamodb(self):
        test_ddb = health._get_dynamodb_status()
        self.assertIn(True, test_ddb)

    @testmode.standalone
    def test_event_relay(self):
        test_er = health._get_event_relay_status()
        self.assertIn(True, test_er)


if __name__ == "__main__":
    unittest.main()
