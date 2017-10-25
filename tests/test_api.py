#!/usr/bin/env python
# coding: utf-8

"""
Functional Test of the API
"""

import os, sys, unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from tests.infra import DSSAssertMixin, DSSUploadMixin, DSSStorageMixin, TestBundle
from tests.infra.server import ThreadedLocalServer


class TestApi(unittest.TestCase, DSSAssertMixin, DSSUploadMixin, DSSStorageMixin):
    @classmethod
    def setUpClass(cls):
        cls.app = ThreadedLocalServer()
        cls.app.start()

    @classmethod
    def tearDownClass(cls):
        cls.app.shutdown()

    def setUp(self):
        self.replica = "aws"
        dss.Config.set_config(dss.BucketConfig.TEST)
        self.blobstore, _, self.bucket = dss.Config.get_cloud_specific_handles(self.replica)

    BUNDLE_FIXTURE = 'fixtures/test_api/bundle'

    def test_creation_and_retrieval_of_files_and_bundle(self):
        """
        Test file and bundle lifecycle.
        Exercises:
          - PUT /files/<uuid>
          - PUT /bundles/<uuid>
          - GET /bundles/<uuid>
          - GET /files/<uuid>
        and checks that data corresponds where appropriate.
        """
        bundle = TestBundle(self.blobstore, self.BUNDLE_FIXTURE, self.bucket, self.replica)
        self.upload_files_and_create_bundle(bundle, self.replica)
        self.get_bundle_and_check_files(bundle, self.replica)


if __name__ == '__main__':
    unittest.main()
