#!/usr/bin/env python
# coding: utf-8

"""
Functional Test of the API
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import os, sys, unittest, uuid, json
import boto3, requests

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from tests.infra import DSSAsserts, UrlBuilder, get_env


class TestApi(unittest.TestCase, DSSAsserts):

    def setUp(self):
        DSSAsserts.setup(self)
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

    def upload_files_and_create_bundle(self, bundle):
        for s3file in bundle.files:
            version = self.upload_file(s3file)
            s3file.version = version
        self.create_bundle(bundle)

    def upload_file(self, bundle_file):
        response = self.assertPutResponse(
            f"/v1/files/{bundle_file.uuid}",
            requests.codes.created,
            json_request_body=dict(
                bundle_uuid=bundle_file.bundle.uuid,
                creator_uid=0,
                source_url=bundle_file.url
            )
        )
        response_data = json.loads(response[1])
        self.assertIs(type(response_data), dict)
        self.assertIn('version', response_data)
        return response_data['version']

    def create_bundle(self, bundle):
        response = self.assertPutResponse(
            str(UrlBuilder().set(path='/v1/bundles/' + bundle.uuid).add_query('replica', 'aws')),
            requests.codes.created,
            json_request_body=self.put_bundle_payload(bundle)
        )
        response_data = json.loads(response[1])
        self.assertIs(type(response_data), dict)
        self.assertIn('version', response_data)
        bundle.version = response_data['version']

    @staticmethod
    def put_bundle_payload(bundle):
        payload = {
            'uuid': bundle.uuid,
            'creator_uid': 1234,
            'version': bundle.version,
            'files': [
                {
                    'indexed': True,
                    'name': bundle_file.name,
                    'uuid': bundle_file.uuid,
                    'version': bundle_file.version
                }
                for bundle_file in bundle.files
            ]
        }
        return payload

    def get_bundle_and_check_files(self, bundle):
        response = self.assertGetResponse(
            str(UrlBuilder().set(path='/v1/bundles/' + bundle.uuid).add_query('replica', 'aws')),
            requests.codes.ok
        )
        response_data = json.loads(response[1])
        self.check_bundle_contains_same_files(bundle, response_data['bundle']['files'])
        self.check_files_are_associated_with_bundle(bundle)

    def check_bundle_contains_same_files(self, bundle, file_metadata):
        self.assertEqual(len(bundle.files), len(file_metadata))
        for bundle_file in bundle.files:
            try:
                filedata = next(data for data in file_metadata if data['uuid'] == bundle_file.uuid)
            except StopIteration:
                self.fail(f"File {bundle_file.uuid} is missing from bundle")
            self.assertEqual(filedata['uuid'], bundle_file.uuid)
            self.assertEqual(filedata['name'], bundle_file.name)
            self.assertEqual(filedata['version'], bundle_file.version)

    def check_files_are_associated_with_bundle(self, bundle):
        for bundle_file in bundle.files:
            response = self.assertGetResponse(
                str(UrlBuilder().set(path='/v1/files/' + bundle_file.uuid).add_query('replica', 'aws')),
                requests.codes.found,
            )
            self.assertEqual(bundle_file.bundle.uuid, response[0].headers['X-DSS-BUNDLE-UUID'])
            self.assertEqual(bundle_file.version, response[0].headers['X-DSS-VERSION'])


class S3TestBundle:
    """
    A test bundle staged in S3

    This class does a little bit of "double duty" as we also use it to store the uuid and versions used with the API
    """
    TEST_FIXTURES_BUCKET = get_env('DSS_S3_TEST_FIXTURES_BUCKET')

    def __init__(self, path, bucket=TEST_FIXTURES_BUCKET):
        self.bucket = boto3.resource('s3').Bucket(bucket)
        self.path = path
        self.files = self.enumerate_bundle_files()
        self.uuid = str(uuid.uuid4())
        self.version = None

    def enumerate_bundle_files(self):
        object_summaries = self.bucket.objects.filter(Prefix=f"{self.path}/")
        return [S3File(objectSummary, self) for objectSummary in object_summaries]


class S3File:
    """
    A test file staged in S3
    """
    def __init__(self, object_summary, bundle):
        self.bundle = bundle
        self.path = object_summary.key
        self.name = os.path.basename(self.path)
        self.url = f"s3://{bundle.bucket.name}/{self.path}"
        self.uuid = str(uuid.uuid4())
        self.version = None


if __name__ == '__main__':
    unittest.main()
