#!/usr/bin/env python
# coding: utf-8

from __future__ import absolute_import, division, print_function, unicode_literals

import os
import sys
import unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, pkg_root)

from blobstore.gcsblobstore import GCSBlobStore # noqa


class TestGCSBlobStore(unittest.TestCase):
    def setUp(self):
        self.credentials = os.path.join(pkg_root, "gcs-credentials.json")

    def tearDown(self):
        pass

    def test_connect(self):
        gcsblobstore = GCSBlobStore("czi-hca-test", self.credentials)
        print(gcsblobstore)


if __name__ == '__main__':
    unittest.main()
