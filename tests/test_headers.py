#!/usr/bin/env python
# coding: utf-8
"""
Test header values.
"""
import datetime
import os
import sys
import unittest
import requests
from unittest import mock

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss import dss_handler
from dss.error import DSSException
from dss.util import UrlBuilder
from dss.util.version import datetime_to_version_format
from tests.infra import DSSAssertMixin, testmode
from tests.infra.server import ThreadedLocalServer
from tests import get_auth_header


@dss_handler
def mock_500_server_error():
    raise DSSException(requests.codes.internal_server_error,
                       "internal_server_error",
                       "Internal Server Error")


@dss_handler
def mock_501_not_implemented():
    raise DSSException(requests.codes.not_implemented,
                       "not_implemented",
                       "Not Implemented")


@dss_handler
def mock_502_bad_gateway():
    raise DSSException(requests.codes.bad_gateway,
                       "bad_gateway",
                       "Bad Gateway")


@dss_handler
def mock_503_service_unavailable():
    raise DSSException(requests.codes.service_unavailable,
                       "service_unavailable",
                       "Service Unavailable")


@dss_handler
def mock_504_gateway_timeout():
    raise DSSException(requests.codes.gateway_timeout,
                       "gateway_timeout",
                       "Gateway Timeout")


@testmode.standalone
class TestRetryAfterHeaders(unittest.TestCase, DSSAssertMixin):
    """Presence or absence of Retry-After headers is defined by dss.error.include_retry_after_header."""
    def test_502_get_bundle_HAS_retry_after_response(self):
        """Mock seems resistant to multiple calls, therefore this is only used for one endpoint."""
        with mock.patch('dss.api.bundles.get', side_effect=DSSException(502, 'bad_gateway', "Bad Gateway")):
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

    def test_500_server_error(self):
        """Test that the dss_handler includes retry-after headers."""
        r = mock_500_server_error()
        self.assertEqual(int(r.headers['Retry-After']), 10)

    def test_501_not_implemented(self):
        """501 should not be retried."""
        r = mock_501_not_implemented()
        self.assertEqual(r.headers.get('Retry-After'), None)

    def test_502_bad_gateway(self):
        """Test that the dss_handler includes retry-after headers."""
        r = mock_502_bad_gateway()
        self.assertEqual(int(r.headers['Retry-After']), 10)

    def test_503_service_unavailable(self):
        """Test that the dss_handler includes retry-after headers."""
        r = mock_503_service_unavailable()
        self.assertEqual(int(r.headers['Retry-After']), 10)

    def test_504_504_gateway_timeout(self):
        """Test that the dss_handler includes retry-after headers."""
        r = mock_504_gateway_timeout()
        self.assertEqual(int(r.headers['Retry-After']), 10)


if __name__ == '__main__':
    unittest.main()
