#!/usr/bin/env python3

import os, unittest, io, json
from uuid import uuid4
from datetime import datetime

import boto3
from botocore.vendored import requests

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

    def upload_file(self, contents):
        s3_test_bucket = get_env("DSS_S3_BUCKET_TEST")
        src_key = generate_test_key()
        s3 = boto3.resource('s3')
        with io.BytesIO(json.dumps(contents).encode()) as fh, ChecksummingSink() as sink:
            sink.write(fh.read())
            sums = sink.get_checksums()
            metadata = {'hca-dss-crc32c': sums['crc32c'].lower(),
                        'hca-dss-s3_etag': sums['s3_etag'].lower(),
                        'hca-dss-sha1': sums['sha1'].lower(),
                        'hca-dss-sha256': sums['sha256'].lower()}
            fh.seek(0)
            s3.Bucket(s3_test_bucket).Object(src_key).upload_fileobj(fh, ExtraArgs={"Metadata": metadata})
        source_url = f"s3://{s3_test_bucket}/{src_key}"
        file_uuid = str(uuid4())
        version = datetime_to_version_format(datetime.utcnow())
        urlbuilder = UrlBuilder().set(path='/v1/files/' + file_uuid)
        urlbuilder.add_query("version", version)

        resp_obj = self.assertPutResponse(str(urlbuilder),
                                          requests.codes.created,
                                          json_request_body=dict(creator_uid=0,
                                                                 source_url=source_url))
        return file_uuid, resp_obj.json["version"]

    def test_collections(self):
        file_uuid, file_version = self.upload_file({"foo": 1})
        col_file_item = dict(type="file", uuid=file_uuid, version=file_version)
        col_ptr_item = dict(type="foo", uuid=file_uuid, version=file_version, fragment="/foo")
        contents = [col_file_item] * 8 + [col_ptr_item] * 8

        with self.subTest("PUT new collection"):
            res = requests.put("http://localhost:5000/v1/collections",
                               headers=get_auth_header(authorized=True),
                               params=dict(uuid=str(uuid4()), version=datetime.now().isoformat(), replica="aws"),
                               json=dict(name="n", description="d", details={}, contents=contents))
            res.raise_for_status()
            uuid, version = res.json()["uuid"], res.json()["version"]
        with self.subTest("GET created collection"):
            res = requests.get("http://localhost:5000/v1/collections/{}".format(uuid),
                               headers=get_auth_header(authorized=True),
                               params=dict(version=version, replica="aws"))
            res.raise_for_status()
            self.assertEqual(res.json()["contents"], contents)
        with self.subTest("GET latest version of collection"):
            res = requests.get("http://localhost:5000/v1/collections/{}".format(uuid),
                               headers=get_auth_header(authorized=True),
                               params=dict(replica="aws"))
            res.raise_for_status()
            self.assertEqual(res.json()["contents"], contents)
        res = requests.get("http://localhost:5000/v1/collections/{}".format(uuid),
                           headers=get_auth_header(authorized=True),
                           params=dict(replica="aws", version="9000"))
        self.assertEqual(res.status_code, requests.codes.not_found)
        res = requests.patch("http://localhost:5000/v1/collections/{}".format(uuid),
                             headers=get_auth_header(authorized=True),
                             params=dict(replica="aws"),
                             json=dict())
        self.assertEqual(res.status_code, requests.codes.bad_request)
        for patch_payload in [dict(),
                              dict(description="foo", name="cn"),
                              dict(description="bar", details={1: 2}),
                              dict(addContents=contents),
                              dict(removeContents=contents)]:
            with self.subTest("PATCH with {}".format(patch_payload)):
                res = requests.patch("http://localhost:5000/v1/collections/{}".format(uuid),
                                     headers=get_auth_header(authorized=True),
                                     params=dict(version=version, replica="aws"),
                                     json=patch_payload)
                res.raise_for_status()
                version = res.json()["version"]
        with self.subTest("Get updated version of collection"):
            res = requests.get("http://localhost:5000/v1/collections/{}".format(uuid),
                               headers=get_auth_header(authorized=True),
                               params=dict(replica="aws"))
            res.raise_for_status()
            collection = res.json()
            del collection["owner"]
            self.assertEqual(collection,
                             dict(contents=[], description='bar', details={"1": 2}, name='cn'))
        with self.subTest("PUT invalid fragment reference"):
            invalid_ptr = dict(type="foo", uuid=file_uuid, version=file_version, fragment="/xyz")
            res = requests.put("http://localhost:5000/v1/collections",
                               headers=get_auth_header(authorized=True),
                               params=dict(uuid=str(uuid4()), version=datetime.now().isoformat(), replica="aws"),
                               json=dict(name="n", description="d", details={}, contents=[invalid_ptr]))
            self.assertEqual(res.status_code, requests.codes.unprocessable_entity)
        with self.subTest("PATCH invalid fragment reference"):
            pass
        with self.subTest("Dedup semantics"):
            pass
        with self.subTest("GET access control"):
            res = requests.get("http://localhost:5000/v1/collections/{}".format(uuid),
                               headers=get_auth_header(authorized=False),
                               params=dict(replica="aws"))
            self.assertEqual(res.status_code, requests.codes.forbidden)
        with self.subTest("PATCH access control"):
            res = requests.patch("http://localhost:5000/v1/collections/{}".format(uuid),
                                 headers=get_auth_header(authorized=False),
                                 params=dict(replica="aws"),
                                 json=dict(description="foo"))
            self.assertEqual(res.status_code, requests.codes.forbidden)
        with self.subTest("DELETE access control"):
            res = requests.delete("http://localhost:5000/v1/collections/{}".format(uuid),
                                  headers=get_auth_header(authorized=False),
                                  params=dict(replica="aws"))
            self.assertEqual(res.status_code, requests.codes.forbidden)
        with self.subTest("Delete collection"):
            res = requests.delete("http://localhost:5000/v1/collections/{}".format(uuid),
                                  headers=get_auth_header(authorized=True),
                                  params=dict(replica="aws"))
            res.raise_for_status()
        with self.subTest("Verify deleted"):
            res = requests.get("http://localhost:5000/v1/collections/{}".format(uuid),
                               headers=get_auth_header(authorized=True),
                               params=dict(replica="aws"))
            self.assertEqual(res.status_code, requests.codes.not_found)

if __name__ == '__main__':
    unittest.main()
