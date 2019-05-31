import os
import unittest


def standalone(f):
    return unittest.skipUnless(is_standalone(), "Skipping standalone test")(f)


def is_standalone():
    return "standalone" in _test_mode()


def integration(f):
    return unittest.skipUnless(is_integration(), "Skipping integration test")(f)


def is_integration():
    return "integration" in _test_mode()


def always(f):
    return f


def _test_mode():
    return os.environ.get('FUS_TEST_MODE', "standalone")
