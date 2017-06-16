#!/usr/bin/env python
# coding: utf-8

from __future__ import absolute_import, division, print_function, unicode_literals

import boto3
import json
import os
import requests
import sys
import unittest
import uuid

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, pkg_root)


class TestExampleBundles(unittest.TestCase):

    DSS_API_URI_BASE = 'https://hca-dss.czi.technology/v1'

    def test_example_bundles(self):
        for bundle in ExampleBundle.all():
            self.progress("Processing bundle %s" % bundle.path)
            self.upload_bundle(bundle)
            self.check_bundle(bundle)

    def upload_bundle(self, bundle):
        for file in bundle.files:
            self.upload_file(file)
        self.create_bundle(bundle)

    def upload_file(self, file):
        self.progress("Uploading file %s" % file.path)
        upload_url = "%s/files/%s" % (self.DSS_API_URI_BASE, file.uuid)
        headers = {'Content-type': 'application/json'}
        payload = self.file_upload_payload(file)
        response = requests.put(upload_url, headers=headers, data=json.dumps(payload))
        self.assertEqual(response.status_code, 201)

    @classmethod
    def progress(cls, message):
        if 'VERBOSE' in os.environ:
            print(message)

    @classmethod
    def file_upload_payload(cls, file):
        return {
            'bundle_uuid': file.bundle.uuid,
            'creator_uid': 0,
            'source_url': file.url,
            'timestamp': file.timestamp.isoformat()
        }

    def create_bundle(self, bundle):
        # TODO when the PUT /bundle/<uuid> API becomes available.
        # {
        #     uuid
        #     creator_uid
        #     bundle_timestamp
        #     contents: [
        #         {
        #             - file_uuid
        #             - timestamp
        #         }
        #     ]
        pass

    def check_bundle(self, bundle):
        # TODO when the API starts actually storing bundles and files.
        # GET /bundle/<uuid>
        # Check bundle contents against what we believe the contents should be.
        # Check each file.
        pass


class ExampleBundle:
    DSS_S3_REGION = 'us-east-1'
    BUNDLE_EXAMPLES_BUCKET = 'hca-dss-test-src'
    BUNDLE_EXAMPLES_ROOT = 'data-bundle-examples'
    BUNDLE_EXAMPLES_BUNDLE_LIST_PATH = "%s/import/bundle_list" % BUNDLE_EXAMPLES_ROOT

    s3client = boto3.client('s3', region_name=DSS_S3_REGION)

    def __init__(self, bundle_path):
        self.path = bundle_path
        self.uuid = str(uuid.uuid4())
        self.files = self.__get_s3_files()

    @classmethod
    def all(cls):
        bundle_list_s3object = cls.s3client.get_object(Bucket=cls.BUNDLE_EXAMPLES_BUCKET,
                                                       Key=cls.BUNDLE_EXAMPLES_BUNDLE_LIST_PATH)
        bundle_list = bundle_list_s3object['Body'].read().decode('utf-8').split("\n")
        for bundle_path in bundle_list:
            yield cls(bundle_path)

    def __get_s3_files(self):
        bundle_folder_path = "%s/%s" % (self.BUNDLE_EXAMPLES_ROOT, self.path)
        response = self.s3client.list_objects(Bucket=self.BUNDLE_EXAMPLES_BUCKET, Prefix=bundle_folder_path)
        return [ExampleFile(s3_file_metadata, self) for s3_file_metadata in response['Contents']]


class ExampleFile:
    def __init__(self, s3_file_metadata, bundle):
        self.bundle = bundle
        self.path = s3_file_metadata['Key']
        self.url = "s3://%s/%s/%s" % (bundle.BUNDLE_EXAMPLES_BUCKET, bundle.BUNDLE_EXAMPLES_ROOT, self.path)
        self.timestamp = s3_file_metadata['LastModified']
        self.uuid = str(uuid.uuid4())
