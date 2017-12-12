import os
import unittest


def standalone(f):
    return unittest.skipUnless(
        os.environ.get('DSS_TEST_MODE', "standalone").find("standalone") != -1,
        "Skipping standalone test")(f)


def integration(f):
    return unittest.skipUnless(
        os.environ.get('DSS_TEST_MODE', "integration").find("integration") != -1,
        "Skipping integration test")(f)
