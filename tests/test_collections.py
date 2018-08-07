#!/usr/bin/env python3
import typing

import itertools
import os, sys, unittest, io, json
from uuid import uuid4
from datetime import datetime

import boto3
from botocore.vendored import requests
from dcplib.s3_multipart import get_s3_multipart_chunk_size

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests import get_auth_header
from tests.infra import generate_test_key, get_env, DSSAssertMixin, DSSUploadMixin
from tests.fixtures.cloud_uploader import ChecksummingSink
from dss.util.version import datetime_to_version_format
from dss.util import UrlBuilder
from tests.infra.server import ThreadedLocalServer


class TestCollections(unittest.TestCase, DSSAssertMixin, DSSUploadMixin):
    @classmethod
    def setUpClass(cls):
        cls.app = ThreadedLocalServer()
        cls.app.start()

        cls.file_uuid, cls.file_version = cls.upload_file(cls.app, {"foo": 1})
        cls.col_file_item = dict(type="file", uuid=cls.file_uuid, version=cls.file_version)
        cls.col_ptr_item = dict(type="foo", uuid=cls.file_uuid, version=cls.file_version, fragment="/foo")
        cls.contents = [cls.col_file_item] * 8 + [cls.col_ptr_item] * 8
        cls.uuid, cls.version = cls._put(cls, cls.contents)
        cls.invalid_ptr = dict(type="foo", uuid=cls.file_uuid, version=cls.file_version, fragment="/xyz")

    @staticmethod
    def upload_file(app, contents):
        s3_test_bucket = get_env("DSS_S3_BUCKET_TEST")
        src_key = generate_test_key()
        s3 = boto3.resource('s3')
        encoded = json.dumps(contents).encode()
        chunk_size = get_s3_multipart_chunk_size(len(encoded))
        with io.BytesIO(encoded) as fh, ChecksummingSink(write_chunk_size=chunk_size) as sink:
            sink.write(fh.read())
            sums = sink.get_checksums()
            metadata = {'hca-dss-crc32c': sums['crc32c'].lower(),
                        'hca-dss-s3_etag': sums['s3_etag'].lower(),
                        'hca-dss-sha1': sums['sha1'].lower(),
                        'hca-dss-sha256': sums['sha256'].lower()}
            fh.seek(0)
            # TODO: consider switching to unmanaged uploader (putobject w/blob)
            s3.Bucket(s3_test_bucket).Object(src_key).upload_fileobj(fh, ExtraArgs={"Metadata": metadata})
        source_url = f"s3://{s3_test_bucket}/{src_key}"
        file_uuid = str(uuid4())
        version = datetime_to_version_format(datetime.utcnow())
        urlbuilder = UrlBuilder().set(path='/v1/files/' + file_uuid)
        urlbuilder.add_query("version", version)

        resp_obj = app.put(str(urlbuilder),
                           json=dict(creator_uid=0,
                           source_url=source_url))
        resp_obj.raise_for_status()
        return file_uuid, resp_obj.json()["version"]

    @classmethod
    def teardownClass(cls):
        cls._delete_collection(cls, cls.uuid)
        cls.app.shutdown()

    def test_put(self):
        "PUT new collection"
        uuid, _ = self._put(self.contents)
        self.addCleanup(self._delete_collection, uuid)

    def test_get(self):
        "GET created collection"
        res = self.app.get("/v1/collections/{}".format(self.uuid),
                           headers=get_auth_header(authorized=True),
                           params=dict(version=self.version, replica="aws"))
        res.raise_for_status()
        self.assertEqual(res.json()["contents"], [self.col_file_item, self.col_ptr_item])

    def test_get_latest(self):
        "GET latest version of collection."
        res = self.app.get("/v1/collections/{}".format(self.uuid),
                           headers=get_auth_header(authorized=True),
                           params=dict(replica="aws"))
        res.raise_for_status()
        self.assertEqual(res.json()["contents"], [self.col_file_item, self.col_ptr_item])

    def test_get_version_not_found(self):
        "NOT FOUND is returned when version does not exist."
        res = self.app.get("/v1/collections/{}".format(self.uuid),
                           headers=get_auth_header(authorized=True),
                           params=dict(replica="aws", version="9000"))
        self.assertEqual(res.status_code, requests.codes.not_found)

    def test_patch_no_version(self):
        "BAD REQUEST is returned when patching without the version."
        res = self.app.patch("/v1/collections/{}".format(self.uuid),
                             headers=get_auth_header(authorized=True),
                             params=dict(replica="aws"),
                             json=dict())
        self.assertEqual(res.status_code, requests.codes.bad_request)

    def test_patch(self):
        col_file_item = dict(type="file", uuid=self.file_uuid, version=self.file_version)
        col_ptr_item = dict(type="foo", uuid=self.file_uuid, version=self.file_version, fragment="/foo")
        contents = [col_file_item] * 8 + [col_ptr_item] * 8
        uuid, version = self._put(contents)

        for patch_payload in [dict(),
                              dict(description="foo", name="cn"),
                              dict(description="bar", details={1: 2}),
                              dict(add_contents=contents),
                              dict(remove_contents=contents)]:
            with self.subTest(patch_payload):
                res = self.app.patch("/v1/collections/{}".format(uuid),
                                     headers=get_auth_header(authorized=True),
                                     params=dict(version=version, replica="aws"),
                                     json=patch_payload)
                res.raise_for_status()
                self.assertNotEqual(version, res.json()["version"])
                version = res.json()["version"]

        with self.subTest("Get updated version of collection"):
            res = self.app.get("/v1/collections/{}".format(uuid),
                               headers=get_auth_header(authorized=True),
                               params=dict(replica="aws"))
            res.raise_for_status()
            collection = res.json()
            del collection["owner"]
            self.assertEqual(collection,
                             dict(contents=[], description='bar', details={"1": 2}, name='cn'))

    def test_put_invalid_fragment(self):
        "PUT invalid fragment reference"
        uuid = str(uuid4())
        self.addCleanup(self._delete_collection, uuid)
        res = self.app.put("/v1/collections",
                           headers=get_auth_header(authorized=True),
                           params=dict(uuid=uuid, version=datetime.now().isoformat(), replica="aws"),
                           json=dict(name="n", description="d", details={}, contents=[self.invalid_ptr] * 128))
        self.assertEqual(res.status_code, requests.codes.unprocessable_entity)

    def test_patch_invalid_fragment(self):
        "PATCH invalid fragment reference"
        res = self.app.patch("/v1/collections/{}".format(self.uuid),
                             headers=get_auth_header(authorized=True),
                             params=dict(version=self.version, replica="aws"),
                             json=dict(add_contents=[self.invalid_ptr] * 256))
        self.assertEqual(res.status_code, requests.codes.unprocessable_entity)

    def test_patch_excessive(self):
        "PATCH excess payload"
        res = self.app.patch("/v1/collections/{}".format(self.uuid),
                             headers=get_auth_header(authorized=True),
                             params=dict(version=self.version, replica="aws"),
                             json=dict(add_contents=[self.col_ptr_item] * 1024))
        self.assertEqual(res.status_code, requests.codes.bad_request)

    def test_patch_missing_params(self):
        missing_params = [dict(replica="aws"),
                          dict(version=self.version),
                          dict(),
                          dict(replica="", version=self.version),
                          dict(replica="aws", version=""),
                          dict(replica="aws", version="GIBBERISH"),
                          ]
        for params in missing_params:
            with self.subTest(params):
                res = self.app.patch("/v1/collections/{}".format(self.uuid),
                                     headers=get_auth_header(authorized=True),
                                     params=params,
                                     json=dict(description="foo"))
                self.assertEqual(res.status_code, requests.codes.bad_request)
        with self.subTest("json_request_body"):
            res = self.app.patch("/v1/collections/{}".format(self.uuid),
                                 headers=get_auth_header(authorized=True),
                                 params=dict(replica='aws', version=self.version))
            self.assertEqual(res.status_code, requests.codes.bad_request)

    def test_get_missing_params(self):
        missing_params = [dict(version=self.version),
                          dict(),
                          dict(replica="", version=self.version)
                          ]
        for params in missing_params:
            with self.subTest(params):
                res = self.app.get("/v1/collections/{}".format(self.uuid),
                                   headers=get_auth_header(authorized=True),
                                   params=params)
                self.assertEqual(res.status_code, requests.codes.bad_request)

    def test_put_missing_params(self):
        uuid = str(uuid4())
        self.addCleanup(self._delete_collection, uuid)
        missing_params = [
            (uuid, None),
            (datetime.now().isoformat(), None),
            ('aws', None)
        ]
        for uuid, version, replica in itertools.product(*missing_params):
            params = {}
            if uuid:
                params['uuid'] = uuid
            if version:
                params['version'] = version
            if replica:
                params['replica'] = replica
            if len(params) == 3:
                continue
            with self.subTest(params):
                res = self.app.put("/v1/collections",
                                   headers=get_auth_header(authorized=True),
                                   params=params,
                                   json=dict(name="n", description="d", details={}, contents=self.contents))
                self.assertEqual(res.status_code, requests.codes.bad_request)

    def test_access_control(self):
        with self.subTest("PUT"):
            uuid = str(uuid4())
            self.addCleanup(self._delete_collection, uuid)
            res = self.app.put("/v1/collections",
                               headers=get_auth_header(authorized=False),
                               params=dict(version=datetime.now().isoformat(),
                                           uuid=uuid,
                                           replica='aws'),
                               json=dict(name="n", description="d", details={}, contents=self.contents))
            self.assertEqual(res.status_code, requests.codes.forbidden)
        with self.subTest("GET"):
            res = self.app.get("/v1/collections/{}".format(self.uuid),
                               headers=get_auth_header(authorized=False),
                               params=dict(replica="aws"))
            self.assertEqual(res.status_code, requests.codes.forbidden)
        with self.subTest("PATCH"):
            res = self.app.patch("/v1/collections/{}".format(self.uuid),
                                 headers=get_auth_header(authorized=False),
                                 params=dict(replica="aws", version=self.version),
                                 json=dict(description="foo"))
            self.assertEqual(res.status_code, requests.codes.forbidden)
        with self.subTest("DELETE"):
            res = self.app.delete("/v1/collections/{}".format(self.uuid),
                                  headers=get_auth_header(authorized=False),
                                  params=dict(replica="aws"))
            self.assertEqual(res.status_code, requests.codes.forbidden)

    def test_delete(self):
        uuid, version = self._put(self.contents)
        with self.subTest("Delete collection"):
            res = self.app.delete("/v1/collections/{}".format(uuid),
                                  headers=get_auth_header(authorized=True),
                                  params=dict(replica="aws"))
            res.raise_for_status()
        with self.subTest("Verify deleted"):
            res = self.app.get("/v1/collections/{}".format(uuid),
                               headers=get_auth_header(authorized=True),
                               params=dict(replica="aws"))
            self.assertEqual(res.status_code, requests.codes.not_found)

    def _put(self, contents: typing.List,
             authorized: bool=True,
             uuid: typing.Optional[str]=None,
             version: typing.Optional[str]=None,
             replica: str='aws') -> typing.Tuple[str, str]:
        uuid = str(uuid4()) if uuid is None else uuid
        version = datetime.now().isoformat() if version is None else version

        params = dict()
        if uuid is not 'missing':
            params['uuid'] = uuid
        if version is not 'missing':
            params['version'] = version
        if replica is not 'missing':
            params['replica'] = replica

        res = self.app.put("/v1/collections",
                           headers=get_auth_header(authorized=authorized),
                           params=params,
                           json=dict(name="n", description="d", details={}, contents=contents))
        res.raise_for_status()
        return res.json()["uuid"], res.json()["version"]

    def _delete_collection(self, uuid: str, replica: str='aws'):
        self.app.delete("/v1/collections/{}".format(uuid),
                        headers=get_auth_header(authorized=True),
                        params=dict(replica=replica))


if __name__ == '__main__':
    unittest.main()
