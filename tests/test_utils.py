#!/usr/bin/env python

import os
import sys
import unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.util.aws import ARN
from tests import infra

infra.start_verbose_logging()

class TestUtils(unittest.TestCase):
    def test_aws_utils(self):
        arn = ARN(service="sns", resource="my_topic")
        arn.get_region()
        arn.get_account_id()
        str(arn)

if __name__ == '__main__':
    unittest.main()
