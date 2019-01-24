import os
import unittest


def standalone(f):
    return unittest.skipUnless(
        "standalone" in os.environ.get('DSS_TEST_MODE', "standalone"),
        "Skipping standalone test")(f)


def integration(f):
    return unittest.skipUnless(
        "integration" in os.environ.get('DSS_TEST_MODE', "integration"),
        "Skipping integration test")(f)
