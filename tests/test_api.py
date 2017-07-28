#!/usr/bin/env python
# coding: utf-8

"""
Functional Test of the API
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import os, sys, unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from tests.infra import DSSAsserts, StorageTestSupport, S3TestBundle


class TestApi(unittest.TestCase, DSSAsserts, StorageTestSupport):

    def setUp(self):
        dss.Config.set_config(dss.DeploymentStage.TEST)
        self.app = dss.create_app().app.test_client()

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
        bundle = S3TestBundle(self.BUNDLE_FIXTURE)
        self.upload_files_and_create_bundle(bundle)
        self.get_bundle_and_check_files(bundle)


if __name__ == '__main__':
    unittest.main()
