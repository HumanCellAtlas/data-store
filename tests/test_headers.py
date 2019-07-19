#!/usr/bin/env python
# coding: utf-8
"""
Test header values.
"""
import datetime
import os
import sys
import unittest
from unittest import mock

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.error import DSSException
from dss.util import UrlBuilder
from dss.util.version import datetime_to_version_format
from tests.infra import DSSAssertMixin, testmode
from tests.infra.server import ThreadedLocalServer
from tests import get_auth_header


@testmode.standalone
class TestRetryAfterHeaders(unittest.TestCase, DSSAssertMixin):
    """Presence or absence of Retry-After headers is defined by dss.error.include_retry_after_header."""
    def test_502_get_bundle_HAS_retry_after_response(self):
        """Mock seems resistant to multiple calls, therefore this is only used for one endpoint."""
        # TODO: Add additional tests as defined in dss.error.include_retry_after_header
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


if __name__ == '__main__':
    unittest.main()
