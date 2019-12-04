import os
import sys
import unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests.infra import testmode
from tests.test_bundle import TestBundleApi
from tests.infra import get_env, DSSUploadMixin, TestAuthMixin, DSSAssertMixin

@testmode.integration
class TestFoobar1(unittest.TestCase):
    def test_foobar1(self):
        print('ohai foobar 1 integration test ran okay')

@testmode.integration
class TestFoobar2(TestBundleApi, TestAuthMixin, DSSAssertMixin, DSSUploadMixin):
    def test_foobar2(self):
        print('ohai foobar 2 integration test ran okay')

if __name__ == '__main__':
    unittest.main()
