import os
import sys
import unittest
import requests

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests.infra import testmode
from tests.infra.server import ThreadedLocalServer, MockFusilladeHandler
import dss.error
from dss.util import security
from dss import BucketConfig, Config, DeploymentStage


def setUpModule():
    MockFusilladeHandler.start_serving()


def tearDownModule():
    MockFusilladeHandler.stop_serving()


class TestMockFusilladeServer(unittest.TestCase):
    """Test that the mock Fusillade server in dss/tests/infra/server.py is functioning properly"""

    def test_get_policy(self):
        actions = ["dss:PutBundle"]
        resources = ["arn:hca:dss:dev:*:bundle/123456/0"]

        # Ensure whitelisted principals are granted access
        for principal in MockFusilladeHandler._whitelist:
            security.assert_authorized(principal, actions, resources)

        # Ensure non-whitelisted principals are denied access
        for principal in ['invalid-email@test-server.data.humancellatlas.org']:
            with self.assertRaises(dss.error.DSSForbiddenException):
                security.assert_authorized(principal, actions, resources)


if __name__ == "__main__":
    unittest.main()
