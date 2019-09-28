import os
import sys
import unittest
import requests

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests.infra import testmode
from tests.infra.server import ThreadedLocalServer
from tests.infra.server import ThreadedMockFusilladeServer as MockFusillade
import dss.error
from dss.util import security
from dss import BucketConfig, Config, DeploymentStage


class TestMockFusilladeServer(unittest.TestCase):
    """Test that the mock Fusillade server in dss/tests/infra/server.py is functioning properly"""

    @classmethod
    def setUpClass(self):
        Config.set_config(BucketConfig.TEST)
        MockFusillade.startServing()
        self.auth_url = MockFusillade.get_endpoint()

    def test_get_policy(self):
        actions = ["dss:PutBundle"]
        resources = ["arn:hca:dss:dev:*:bundle/123456/0"]

        # Ensure whitelisted principals are granted access
        for principal in MockFusillade._whitelist:
            security.assert_authorized(principal, actions, resources)

        # Ensure blacklisted principals are denied access
        for principal in MockFusillade._blacklist:
            with self.assertRaises(dss.error.DSSForbiddenException):
                security.assert_authorized(principal, actions, resources)

    @classmethod
    def tearDownClass(self):
        MockFusillade.stopServing()


if __name__ == "__main__":
    unittest.main()
