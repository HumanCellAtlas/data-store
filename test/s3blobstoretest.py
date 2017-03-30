#!/usr/bin/env python
# coding: utf-8

from __future__ import absolute_import, division, print_function, unicode_literals

import os
import sys
import unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, pkg_root)

from blobstore.s3blobstore import S3BlobStore


class TestS3BlobStore(unittest.TestCase):
    def setUp(self):
        self.aws_access_key = os.getenv['AWS_ACCESS_KEY']
        self.aws_secret_key = os.getenv['AWS_SECRET_KEY']

    def tearDown(self):
        pass

    def test_connect(self):
        s3blobstore = S3BlobStore("czi-hca-test", self.aws_access_key, self.aws_secret_key)

if __name__ == '__main__':
    unittest.main()
