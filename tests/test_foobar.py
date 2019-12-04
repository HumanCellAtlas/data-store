import os
import sys
import unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests.infra import testmode
from tests.test_bundle import TestBundleApi, TestBundleApiMixin
from tests.infra import get_env, DSSUploadMixin, TestAuthMixin, DSSAssertMixin

@testmode.integration
class TestFoobar1(unittest.TestCase):
    def test_foobar1(self):
        print('ohai foobar 1 integration test ran okay')

@testmode.integration
class TestFoobar2(TestBundleApi, TestAuthMixin, DSSAssertMixin, DSSUploadMixin):
    def test_foobar2(self):
        print('ohai foobar 2 integration test ran okay')

@testmode.integration
class TestFoobar2A(TestBundleApi):
    def test_foobar2A(self):
        print('ohai foobar 2A integration test ran okay')

@testmode.integration
class TestFoobar2B(unittest.TestCase, TestAuthMixin):
    def test_foobar2B(self):
        print('ohai foobar 2B integration test ran okay')

@testmode.integration
class TestFoobar2C(unittest.TestCase, DSSAssertMixin):
    def test_foobar2C(self):
        print('ohai foobar 2C integration test ran okay')

@testmode.integration
class TestFoobar2D(unittest.TestCase, DSSUploadMixin):
    def test_foobar2D(self):
        print('ohai foobar 2D integration test ran okay')

@testmode.integration
class TestFoobar3(TestBundleApiMixin):
    def test_foobar3(self):
        print('ohai foobar 3 integration test ran okay')

if __name__ == '__main__':
    unittest.main()
