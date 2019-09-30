import os
import sys
import unittest
import requests

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests.infra import testmode
from tests.infra.server import ThreadedLocalServer
from tests.infra.mock_fusillade import (
    start_multiprocess_mock_fusillade_server,
    stop_multiprocess_mock_fusillade_server,
    MockFusilladeServer,
)
import dss.error
from dss.util import security
from dss import BucketConfig, Config, DeploymentStage


class TestMockFusilladeServer(unittest.TestCase):
    """Test that the mock Fusillade server in dss/tests/infra/server.py is functioning properly"""

    @classmethod
    def setUpClass(self):
        Config.set_config(BucketConfig.TEST)
        start_multiprocess_mock_fusillade_server()
        self.auth_url = MockFusilladeServer.get_endpoint()

    def test_get_policy(self):
        actions = ["dss:PutBundle"]
        resources = ["arn:hca:dss:dev:*:bundle/123456/0"]

        # Ensure whitelisted principals are granted access
        for principal in MockFusilladeServer._whitelist:
            security.assert_authorized(principal, actions, resources)

        # Ensure non-whitelisted principals are denied access
        for principal in ['invalid@email.com']:
            with self.assertRaises(dss.error.DSSForbiddenException):
                security.assert_authorized(principal, actions, resources)

    @classmethod
    def tearDownClass(self):
        stop_multiprocess_mock_fusillade_server()


if __name__ == "__main__":
    unittest.main()
