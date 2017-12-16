#!/usr/bin/env python

import os
import sys
import unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.util import UrlBuilder
from dss.util.aws import ARN, get_s3_chunk_size
from tests.infra import logging, testmode

logging.start_verbose_logging()


class TestAwsUtils(unittest.TestCase):
    @testmode.standalone
    def test_aws_utils(self):
        arn = ARN(service="sns", resource="my_topic")
        arn.get_region()
        arn.get_account_id()
        str(arn)

    @testmode.standalone
    def test_s3_chunk_size(self):
        self.assertEqual(get_s3_chunk_size(10000 * 64 * 1024 * 1024 - 1), 64 * 1024 * 1024)
        self.assertEqual(get_s3_chunk_size(10000 * 64 * 1024 * 1024 + 0), 64 * 1024 * 1024)
        self.assertEqual(get_s3_chunk_size(10000 * 64 * 1024 * 1024 + 1), 65 * 1024 * 1024)
        self.assertEqual(get_s3_chunk_size(10000 * 65 * 1024 * 1024 - 1), 65 * 1024 * 1024)
        self.assertEqual(get_s3_chunk_size(10000 * 65 * 1024 * 1024 + 0), 65 * 1024 * 1024)
        self.assertEqual(get_s3_chunk_size(10000 * 65 * 1024 * 1024 + 1), 66 * 1024 * 1024)


class TestUrlBuilder(unittest.TestCase):
    @testmode.standalone
    def test_simple(self):
        builder = UrlBuilder().set(
            scheme="https",
            netloc="humancellatlas.org",
            path="/abc",
            query=[
                ("ghi", "1"),
                ("ghi", "2"),
            ],
            fragment="def")
        self.assertEqual("https://humancellatlas.org/abc?ghi=1&ghi=2#def", str(builder))

    @testmode.standalone
    def test_has_query(self):
        builder = UrlBuilder().set(
            scheme="https",
            netloc="humancellatlas.org",
            path="/abc",
            query=[
                ("ghi", "1"),
                ("ghi", "2"),
            ],
            fragment="def")
        self.assertTrue(builder.has_query("ghi"))
        self.assertFalse(builder.has_query("abc"))

    @testmode.standalone
    def test_add_query(self):
        builder = UrlBuilder().set(
            scheme="https",
            netloc="humancellatlas.org",
            path="/abc",
            query=[
                ("ghi", "1"),
                ("ghi", "2"),
            ],
            fragment="def")
        self.assertTrue(builder.has_query("ghi"))
        self.assertFalse(builder.has_query("abc"))

        self.assertEqual("https://humancellatlas.org/abc?ghi=1&ghi=2#def", str(builder))

        builder.add_query("abc", "3")
        self.assertTrue(builder.has_query("ghi"))
        self.assertTrue(builder.has_query("abc"))

        self.assertEqual("https://humancellatlas.org/abc?ghi=1&ghi=2&abc=3#def", str(builder))


if __name__ == '__main__':
    unittest.main()
