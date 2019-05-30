#!/usr/bin/env python
# coding: utf-8
"""
Test header values across multiple endpoints.
"""
import datetime
import os
import sys
import unittest
import requests
from unittest import mock

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.error import DSSException
from dss.util import UrlBuilder
from dss.util.version import datetime_to_version_format
from dss.config import DeploymentStage
from tests.infra import DSSAssertMixin, testmode, ExpectedErrorFields
from tests.infra.server import ThreadedLocalServer
from tests import get_auth_header


@testmode.standalone
class TestRetryAfterHeaders(unittest.TestCase, DSSAssertMixin):
    """Presence or absence of Retry-After headers is defined by dss.error.include_retry_after_header."""
    @unittest.skipIf(DeploymentStage.IS_PROD(), "Skipping synthetic 504 test for PROD.")
    def test_504_retry_after_response(self):
        self.app = ThreadedLocalServer()
        self.app.start()
        self.app._chalice_app._override_exptime_seconds = 15.0
        uuid = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
        version = datetime_to_version_format(datetime.datetime.utcnow())

        with self.subTest('Retry-After headers are included in a GET /v1/bundles/{uuid} 504 response.'):
            url = str(UrlBuilder().set(path=f"/v1/bundles/{uuid}")
                      .add_query("version", version)
                      .add_query("replica", 'aws'))

            r = self.assertGetResponse(
                url,
                504,
                expected_error=ExpectedErrorFields(
                    code="timed_out",
                    status=requests.codes.gateway_timeout,
                ),
                headers={"DSS_FAKE_504_PROBABILITY": "1.0"}
            )
            self.assertEqual(int(r.response.headers['Retry-After']), 10)

        with self.subTest('Retry-After headers are NOT included in a POST /v1/bundles/{uuid}/checkout 504 response.'):
            url = str(UrlBuilder().set(path=f"/v1/bundles/{uuid}/checkout")
                      .add_query("version", version)
                      .add_query("replica", 'aws'))

            r = self.assertPostResponse(
                url,
                504,
                expected_error=ExpectedErrorFields(
                    code="timed_out",
                    status=requests.codes.gateway_timeout,
                ),
                headers={"DSS_FAKE_504_PROBABILITY": "1.0"}
            )
            self.assertTrue('Retry-After' not in r.response.headers)
        self.app.shutdown()

    def test_502_get_bundle_HAS_retry_after_response(self):
        """Mock seems resistant to multiple calls, therefore this is only used for one endpoint."""
        # TODO: Add additional tests as defined in dss.error.include_retry_after_header
        with mock.patch('dss.api.bundles.get') as foo:
            foo.side_effect = DSSException(502, 'bad_gateway', "Bad Gateway")
            self.app = ThreadedLocalServer()
            self.app.start()
            uuid = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
            version = datetime_to_version_format(datetime.datetime.utcnow())

            url = str(UrlBuilder().set(path=f"/v1/bundles/{uuid}")
                      .add_query("version", version)
                      .add_query("replica", 'aws'))

            r = self.assertGetResponse(url, 502, headers=get_auth_header())
            self.assertEqual(int(r.response.headers['Retry-After']), 10)
            self.app.shutdown()


if __name__ == '__main__':
    unittest.main()
