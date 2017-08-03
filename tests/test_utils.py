#!/usr/bin/env python

import os
import sys
import unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.util.aws import ARN, get_s3_chunk_size
from tests import infra

infra.start_verbose_logging()

class TestUtils(unittest.TestCase):
    def test_aws_utils(self):
        arn = ARN(service="sns", resource="my_topic")
        arn.get_region()
        arn.get_account_id()
        str(arn)

    def test_s3_chunk_size(self):
        self.assertEqual(get_s3_chunk_size(10000 * 64 * 1024 * 1024 - 1), 64 * 1024 * 1024)
        self.assertEqual(get_s3_chunk_size(10000 * 64 * 1024 * 1024 + 0), 64 * 1024 * 1024)
        self.assertEqual(get_s3_chunk_size(10000 * 64 * 1024 * 1024 + 1), 65 * 1024 * 1024)
        self.assertEqual(get_s3_chunk_size(10000 * 65 * 1024 * 1024 - 1), 65 * 1024 * 1024)
        self.assertEqual(get_s3_chunk_size(10000 * 65 * 1024 * 1024 + 0), 65 * 1024 * 1024)
        self.assertEqual(get_s3_chunk_size(10000 * 65 * 1024 * 1024 + 1), 66 * 1024 * 1024)

if __name__ == '__main__':
    unittest.main()
