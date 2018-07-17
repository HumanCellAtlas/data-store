import json

import datetime
import os
import typing
import uuid

import requests
from cloud_blobstore import BlobStore

from dss import Config, Replica
from dss.util import UrlBuilder
from dss.util.version import datetime_to_version_format


class TestBundle:
    def __init__(self, handle: BlobStore, path: str, bucket: str, replica: Replica = Replica.aws,
                 bundle_uuid: str = None) -> None:
        self.path = path
        self.uuid = bundle_uuid if bundle_uuid else str(uuid.uuid4())
        self.version = datetime_to_version_format(datetime.datetime.utcnow())
        self.handle = handle
        self.bucket = bucket
        self.files = self.enumerate_bundle_files(replica)

    def enumerate_bundle_files(self, replica: Replica) -> list:
        file_keys = self.handle.list(self.bucket, prefix=f"{self.path}/")
        return [TestFile(file_key, self, replica) for file_key in file_keys]


class TestFile:
    def __init__(self, file_key, bundle, replica: Replica) -> None:
        self.metadata = bundle.handle.get_user_metadata(bundle.bucket, file_key)
        self.indexed = bundle.handle.get_content_type(bundle.bucket, file_key) == "application/json"
        self.name = os.path.basename(file_key)
        self.path = file_key
        self.uuid = str(uuid.uuid4())
        self.url = replica.storage_schema + "://" + bundle.bucket + "/" + self.path
        self.version = None


class DSSStorageMixin:

    """
    Storage test operations for files and bundles.

    This class is a mixin like DSSAssertMixin, and like DSSAssertMixin, expects the client app to be available as
    'self.app'
    """

    def upload_files_and_create_bundle(self, bundle: TestBundle, replica: Replica):
        for file in bundle.files:
            version = self.upload_file(file, replica)
            file.version = version
        self.create_bundle(bundle, replica)

    def upload_file(self: typing.Any, bundle_file: TestFile, replica: Replica) -> str:
        response = self.upload_file_wait(
            bundle_file.url,
            replica,
            file_uuid=bundle_file.uuid
        )
        response_data = json.loads(response[1])
        self.assertIs(type(response_data), dict)
        self.assertIn('version', response_data)
        return response_data['version']

    def create_bundle(self: typing.Any, bundle: TestBundle, replica: Replica):
        response = self.assertPutResponse(
            str(UrlBuilder().set(path='/v1/bundles/' + bundle.uuid)
                .add_query('replica', replica.name).add_query('version', bundle.version)),
            requests.codes.created,
            json_request_body=self.put_bundle_payload(bundle)
        )
        response_data = json.loads(response[1])
        self.assertIs(type(response_data), dict)
        self.assertIn('version', response_data)

    @staticmethod
    def put_bundle_payload(bundle: TestBundle):
        payload = {
            'uuid': bundle.uuid,
            'creator_uid': 1234,
            'version': bundle.version,
            'files': [
                {
                    'indexed': bundle_file.indexed,
                    'name': bundle_file.name,
                    'uuid': bundle_file.uuid,
                    'version': bundle_file.version
                }
                for bundle_file in bundle.files
            ]
        }
        return payload

    def get_bundle_and_check_files(self: typing.Any, bundle: TestBundle, replica: Replica):
        response = self.assertGetResponse(
            str(UrlBuilder().set(path='/v1/bundles/' + bundle.uuid)
                .add_query('replica', replica.name)),
            requests.codes.ok
        )
        response_data = json.loads(response[1])
        self.check_bundle_contains_same_files(bundle, response_data['bundle']['files'])
        self.check_files_are_associated_with_bundle(bundle, replica)

    def check_bundle_contains_same_files(self: typing.Any, bundle: TestBundle, file_metadata: dict):
        self.assertEqual(len(bundle.files), len(file_metadata))
        for bundle_file in bundle.files:
            try:
                filedata = next(data for data in file_metadata if data['uuid'] == bundle_file.uuid)
            except StopIteration:
                self.fail(f"File {bundle_file.uuid} is missing from bundle")
            self.assertEqual(filedata['uuid'], bundle_file.uuid)
            self.assertEqual(filedata['name'], bundle_file.name)
            self.assertEqual(filedata['version'], bundle_file.version)

    def check_files_are_associated_with_bundle(self: typing.Any, bundle: TestBundle, replica: Replica):
        for bundle_file in bundle.files:
            response = self.assertGetResponse(
                str(UrlBuilder().set(path='/v1/files/' + bundle_file.uuid)
                    .add_query('replica', replica.name)),
                requests.codes.found,
            )
            self.assertEqual(bundle_file.version, response[0].headers['X-DSS-VERSION'])
