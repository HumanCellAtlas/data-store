import functools
import json
import logging
import os
import re
import typing
import urllib.parse
import uuid

import boto3
import requests
from flask import wrappers


def start_verbose_logging():
    logging.basicConfig(level=logging.INFO)
    for logger_name in logging.Logger.manager.loggerDict:  # type: ignore
        if logger_name.startswith("botocore") or logger_name.startswith("boto3.resources"):
            logging.getLogger(logger_name).setLevel(logging.WARNING)


def get_env(varname):
    if varname not in os.environ:
        raise RuntimeError(
            "Please set the {} environment variable".format(varname))
    return os.environ[varname]


class DSSAsserts:
    def setup(self):
        self.sre = re.compile("^assert(.+)Response")

    def assertResponse(
            self,
            method: str,
            path: str,
            expected_code: int,
            json_request_body: typing.Optional[dict]=None,
            **kwargs) -> typing.Tuple[wrappers.Response, str, typing.Optional[dict]]:
        """
        Make a request given a HTTP method and a path.  The HTTP status code is checked against `expected_code`.

        If json_request_body is provided, it is serialized and set as the request body, and the content-type of the
        request is set to application/json.

        The first element of the return value is the response object.  The second element of the return value is the
        response text.

        If `parse_response_as_json` is true, then attempt to parse the response body as JSON and return that as the
        third element of the return value.  Otherwise, the third element of the return value is None.
        """
        if json_request_body is not None:
            if 'data' in kwargs:
                self.fail("both json_input and data are defined")
            kwargs['data'] = json.dumps(json_request_body)
            kwargs['content_type'] = 'application/json'

        response = getattr(self.app, method)(path, **kwargs)
        self.assertEqual(response.status_code, expected_code)

        try:
            actual_json = json.loads(response.data.decode("utf-8"))
        except Exception:
            actual_json = None
        return response, response.data, actual_json

    def assertHeaders(
            self,
            response: wrappers.Response,
            expected_headers: dict = {}) -> None:
        for header_name, header_value in expected_headers.items():
            self.assertEqual(response.headers[header_name], header_value)

    # this allows for assert*Response, where * = the request method.
    def __getattr__(self, item: str) -> typing.Any:
        if item.startswith("assert"):
            mo = self.sre.match(item)
            if mo is not None:
                method = mo.group(1).lower()
                return functools.partial(self.assertResponse, method)

        if hasattr(super(DSSAsserts, self), '__getattr__'):
            return super(DSSAsserts, self).__getattr__(item)  # type: ignore
        else:
            raise AttributeError(item)


class S3TestBundle:
    """
    A test bundle staged in S3

    This class does a little bit of "double duty" as we also use it to store the uuid and versions used with the API
    """
    BUCKET_TEST_FIXTURES = get_env('DSS_S3_BUCKET_TEST_FIXTURES')

    def __init__(self, path, bucket=BUCKET_TEST_FIXTURES):
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
        self.metadata = object_summary.Object().metadata
        self.indexed = True if self.metadata['hca-dss-content-type'] == "application/json" else False
        self.name = os.path.basename(self.path)
        self.url = f"s3://{bundle.bucket.name}/{self.path}"
        self.uuid = str(uuid.uuid4())
        self.version = None


class StorageTestSupport(DSSAsserts):

    """
    Storage test operations for files and bundles.

    This class extends DSSAsserts, and like DSSAsserts,
    expects the client app to be available as 'self.app'
    """

    def upload_files_and_create_bundle(self, bundle: S3TestBundle):
        for s3file in bundle.files:
            version = self.upload_file(s3file)
            s3file.version = version
        self.create_bundle(bundle)

    def upload_file(self, bundle_file: S3File) -> str:
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

    def create_bundle(self, bundle: S3TestBundle):
        response = self.assertPutResponse(
            str(UrlBuilder().set(path='/v1/bundles/' + bundle.uuid)
                .add_query('replica', 'aws')),
            requests.codes.created,
            json_request_body=self.put_bundle_payload(bundle)
        )
        response_data = json.loads(response[1])
        self.assertIs(type(response_data), dict)
        self.assertIn('version', response_data)
        bundle.version = response_data['version']

    @staticmethod
    def put_bundle_payload(bundle: S3TestBundle):
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

    def get_bundle_and_check_files(self, bundle: S3TestBundle):
        response = self.assertGetResponse(
            str(UrlBuilder().set(path='/v1/bundles/' + bundle.uuid)
                .add_query('replica', 'aws')),
            requests.codes.ok
        )
        response_data = json.loads(response[1])
        self.check_bundle_contains_same_files(bundle, response_data['bundle']['files'])
        self.check_files_are_associated_with_bundle(bundle)

    def check_bundle_contains_same_files(self, bundle: S3TestBundle, file_metadata: dict):
        self.assertEqual(len(bundle.files), len(file_metadata))
        for bundle_file in bundle.files:
            try:
                filedata = next(data for data in file_metadata if data['uuid'] == bundle_file.uuid)
            except StopIteration:
                self.fail(f"File {bundle_file.uuid} is missing from bundle")
            self.assertEqual(filedata['uuid'], bundle_file.uuid)
            self.assertEqual(filedata['name'], bundle_file.name)
            self.assertEqual(filedata['version'], bundle_file.version)

    def check_files_are_associated_with_bundle(self, bundle: S3TestBundle):
        for bundle_file in bundle.files:
            response = self.assertGetResponse(
                str(UrlBuilder().set(path='/v1/files/' + bundle_file.uuid)
                    .add_query('replica', 'aws')),
                requests.codes.found,
            )
            self.assertEqual(bundle_file.bundle.uuid, response[0].headers['X-DSS-BUNDLE-UUID'])
            self.assertEqual(bundle_file.version, response[0].headers['X-DSS-VERSION'])


class UrlBuilder:
    def __init__(self):
        self.splitted = urllib.parse.SplitResult("", "", "", "", "")
        self.query = list()

    def set(self, scheme: str=None, netloc: str=None, path: str=None, fragment: str=None) -> "UrlBuilder":
        kwargs = dict()
        if scheme is not None:
            kwargs['scheme'] = scheme
        if netloc is not None:
            kwargs['netloc'] = netloc
        if path is not None:
            kwargs['path'] = path
        if fragment is not None:
            kwargs['fragment'] = fragment
        self.splitted = self.splitted._replace(**kwargs)

        return self

    def add_query(self, query_name: str, query_value: str) -> "UrlBuilder":
        self.query.append((query_name, query_value))

        return self

    def __str__(self) -> str:
        result = self.splitted._replace(query=urllib.parse.urlencode(self.query, doseq=True))

        return urllib.parse.urlunsplit(result)
