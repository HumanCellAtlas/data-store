#!/usr/bin/env python
# coding: utf-8

"""
Functional Test of the API
"""

import os
import sys
import unittest

import requests

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.config import Replica
import dss
from dss.util import UrlBuilder
from tests.infra import DSSAssertMixin, DSSUploadMixin, DSSStorageMixin, TestBundle, testmode, ExpectedErrorFields
from tests.infra.server import ThreadedLocalServer
from tests import get_auth_header


class TestApi(unittest.TestCase, DSSAssertMixin, DSSUploadMixin, DSSStorageMixin):
    @classmethod
    def setUpClass(cls):
        os.environ['DSS_VERSION'] = "test_version"
        cls.app = ThreadedLocalServer()
        cls.app.start()

    @classmethod
    def tearDownClass(cls):
        cls.app.shutdown()

    def setUp(self):
        self.replica = Replica.aws
        dss.Config.set_config(dss.BucketConfig.TEST)
        self.blobstore = dss.Config.get_blobstore_handle(self.replica)
        self.bucket = self.replica.bucket

    BUNDLE_FIXTURE = "fixtures/example_bundle"

    @testmode.standalone
    def test_creation_and_retrieval_of_files_and_bundle(self):

        # FIXME: This test doesn't do much because it uses the test bucket which lacks the fixtures for the test bundle.
        # FIMXE: In particular it does not test any of the /files routes. (hannes)

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

    @testmode.standalone
    def test_get_version(self):
        """
        Test /version endpoint and configuration
        """
        res = self.assertGetResponse("/version", requests.codes.ok)
        self.assertEquals(res.json['version_info']['version'], os.environ['DSS_VERSION'])

    @testmode.standalone
    def test_read_only(self):
        os.environ['DSS_READ_ONLY_MODE'] = "True"
        uuid = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"

        for path in ["bundles", "files", "subscriptions", "collections"]:
            for replica in ["aws", "gcp"]:
                with self.subTest(path=path, replica=replica):
                    url = str(UrlBuilder().set(path=f"/v1/{path}/" + uuid)
                              .add_query("version", "asdf")
                              .add_query("replica", replica))
                    self.assertPutResponse(
                        url,
                        requests.codes.not_allowed,
                        json_request_body=dict(
                            files=[],
                            creator_uid=12345,
                            es_query={},
                            callback_url="https://take.me.to.funkytown",
                            source_url="s3://urlurlurlbaby",
                            description="supercalifragilisticexpialidocious",
                            details={},
                            contents=[]
                        )
                    )

        del os.environ['DSS_READ_ONLY_MODE']

if __name__ == '__main__':
    unittest.main()
