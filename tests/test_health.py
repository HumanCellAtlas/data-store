import os
import sys
import unittest
from unittest import mock
import logging

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.api import health
from tests.infra import testmode
from tests.es import ElasticsearchServer

logger = logging.getLogger(__name__)


@testmode.integration
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

    def test_elastic_search(self):
        test_es = health._get_es_status(port=os.getenv("DSS_ES_PORT"))
        self.assertIn(True, test_es)

    def test_dynamodb(self):
        test_ddb = health._get_dynamodb_status()
        self.assertIn(True, test_ddb)

    def test_event_relay(self):
        test_er = health._get_event_relay_status()
        self.assertIn(True, test_er)

    @mock.patch("dss.api.health._get_es_status")
    @mock.patch("dss.api.health._get_dynamodb_status")
    @mock.patch("dss.api.health._get_event_relay_status")
    def test_full_health_check(self, mock_er, mock_ddb, mock_es):
        healthy_res = {"Healthy": True}
        mock_es.return_value = (True, None)
        mock_ddb.return_value = (True, None)
        mock_er.return_value = (True, None)
        mock_res = health.l2_health_checks()
        self.assertDictEqual(healthy_res, mock_res)

    @testmode.standalone
    def test_resource_fetch(self):

        service_tags = {"Key": "service", "Values": ["dss"]}
        resource_list = health.get_resource_by_tag(resource_string='dynamodb:table', tag_filter=service_tags)
        ddb_tables = [x['ResourceARN'].split('/')[1] for x in resource_list['ResourceTagMappingList'] if
                      os.environ.get('DSS_DEPLOYMENT_STAGE') in x['ResourceARN']]
        print(ddb_tables)
        self.assertGreater(len(ddb_tables), 0)


if __name__ == "__main__":
    unittest.main()
