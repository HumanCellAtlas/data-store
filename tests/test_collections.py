#!/usr/bin/env python3
import typing
import itertools
import os
import sys
import unittest
import io
import json
import boto3

from uuid import uuid4
from datetime import datetime
from requests.utils import parse_header_links
from botocore.vendored import requests
from dcplib.s3_multipart import get_s3_multipart_chunk_size
from urllib.parse import parse_qsl, urlparse, urlsplit

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests import get_auth_header
from tests.infra import generate_test_key, get_env, DSSAssertMixin, DSSUploadMixin, testmode
from tests.infra.server import ThreadedLocalServer
from tests.fixtures.cloud_uploader import ChecksummingSink
from dss.util.version import datetime_to_version_format
from dss.util import UrlBuilder


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
        cls.replicas = ('aws', 'gcp')

    @classmethod
    def teardownClass(cls):
        cls._delete_collection(cls, cls.uuid)
        cls.app.shutdown()

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
                           source_url=source_url),
                           headers=get_auth_header())
        resp_obj.raise_for_status()
        return file_uuid, resp_obj.json()["version"]

    def _test_collection_get_paging(self, codes, replica: str, per_page: int, fetch_all: bool=False):
        """
        Attempts to ensure that a GET /collections call responds with a 206.

        If unsuccessful on the first attempt, temp collections will be added for the user to ensure a response
        and the GET /collections will be reattempted.
        """
        paging_res = self._fetch_collection_get_paging_responses(codes, replica, per_page, fetch_all)

        # only create a ton of collections when not enough collections exist in the bucket to elicit a paging response
        if not paging_res:
            self.create_temp_user_collections(num=per_page + 1)  # guarantee a paging response
            paging_res = self._fetch_collection_get_paging_responses(codes, replica, per_page, fetch_all)
            self.assertTrue(paging_res)

    def create_temp_user_collections(self, num: int):
        for i in range(num):
            contents = [self.col_file_item, self.col_ptr_item]
            uuid, _ = self._put(contents)
            self.addCleanup(self._delete_collection, uuid)

    def _fetch_collection_get_paging_responses(self, codes, replica: str, per_page: int, fetch_all: bool):
        """
        GET /collections and iterate through the paging responses containing all of a user's collections.

        If fetch_all is not True, this will return as soon as it gets one successful 206 paging reply.
        """
        url = UrlBuilder().set(path="/v1/collections/")
        url.add_query("replica", replica)
        url.add_query("per_page", str(per_page))
        resp_obj = self.assertGetResponse(str(url), codes, headers=get_auth_header(authorized=True))
        link_header = resp_obj.response.headers.get('Link')

        paging_res, normal_res = None, None
        while link_header:
            link = parse_header_links(link_header)[0]
            self.assertEquals(link['rel'], 'next')
            parsed = urlsplit(link['url'])
            url = str(UrlBuilder().set(path=parsed.path, query=parse_qsl(parsed.query), fragment=parsed.fragment))
            resp_obj = self.assertGetResponse(url,
                                              expected_code=codes,
                                              headers=get_auth_header(authorized=True))
            link_header = resp_obj.response.headers.get('Link')

            # Make sure we're getting the expected response status code
            if link_header:
                self.assertEqual(resp_obj.response.status_code, requests.codes.partial)
                paging_res = True
                if not fetch_all:
                    return paging_res
            else:
                self.assertEqual(resp_obj.response.status_code, requests.codes.ok)
                normal_res = True
        if fetch_all:
            self.assertTrue(normal_res)
        return paging_res

    @testmode.standalone
    def test_get(self):
        """GET a list of all collections belonging to the user."""
        res = self.app.get('/v1/collections',
                           headers=get_auth_header(authorized=True),
                           params=dict(replica='aws'))
        res.raise_for_status()
        self.assertIn('collections', res.json())

    @testmode.standalone
    def test_collection_paging(self):
        # seems to take about 15 seconds per page when "per_page" == 100
        # so this scales linearly with the total number of collections in the bucket
        # slow because the collection API has to open ALL collections files in the bucket
        # since it cannot determine the owner without opening the file
        # TODO collections desperately need indexing to run in a reasonable amount of time
        codes = {requests.codes.ok, requests.codes.partial}
        for replica in ['aws']:  # TODO: change ['aws'] to self.replicas when GET collections is faster (indexed)
            for per_page in [50, 100]:
                with self.subTest(replica=replica, per_page=per_page):
                    # only check a full run if per_page == 100 because it takes forever
                    fetch_all = True if per_page == 100 else False
                    self._test_collection_get_paging(codes=codes,
                                                     replica=replica,
                                                     per_page=per_page,
                                                     fetch_all=fetch_all)

    def test_collection_paging_too_small(self):
        """Should NOT be able to use a too-small per_page."""
        for replica in self.replicas:
            with self.subTest(replica):
                self._test_collection_get_paging(replica=replica, per_page=49, codes=requests.codes.bad_request)

    def test_collection_paging_too_large(self):
        """Should NOT be able to use a too-large per_page."""
        for replica in self.replicas:
            with self.subTest(replica):
                self._test_collection_get_paging(replica=replica, per_page=101, codes=requests.codes.bad_request)

    def test_put(self):
        """PUT new collection."""
        with self.subTest("with unique contents"):
            contents = [self.col_file_item, self.col_ptr_item]
            uuid, _ = self._put(contents)
            self.addCleanup(self._delete_collection, uuid)
            res = self.app.get("/v1/collections/{}".format(self.uuid),
                               headers=get_auth_header(authorized=True),
                               params=dict(replica="aws"))
            self.assertEqual(res.json()["contents"], [self.col_file_item, self.col_ptr_item])

        with self.subTest("with duplicated contents."):
            uuid, _ = self._put(self.contents)
            self.addCleanup(self._delete_collection, uuid)
            res = self.app.get("/v1/collections/{}".format(self.uuid),
                               headers=get_auth_header(authorized=True),
                               params=dict(replica="aws"))
            self.assertEqual(res.json()["contents"], [self.col_file_item, self.col_ptr_item])

    def test_get_uuid(self):
        """GET created collection."""
        res = self.app.get("/v1/collections/{}".format(self.uuid),
                           headers=get_auth_header(authorized=True),
                           params=dict(version=self.version, replica="aws"))
        res.raise_for_status()
        self.assertEqual(res.json()["contents"], [self.col_file_item, self.col_ptr_item])

    def test_get_uuid_latest(self):
        """GET latest version of collection."""
        res = self.app.get("/v1/collections/{}".format(self.uuid),
                           headers=get_auth_header(authorized=True),
                           params=dict(replica="aws"))
        res.raise_for_status()
        self.assertEqual(res.json()["contents"], [self.col_file_item, self.col_ptr_item])

    def test_get_version_not_found(self):
        """NOT FOUND is returned when version does not exist."""
        res = self.app.get("/v1/collections/{}".format(self.uuid),
                           headers=get_auth_header(authorized=True),
                           params=dict(replica="aws", version="9000"))
        self.assertEqual(res.status_code, requests.codes.not_found)

    def test_patch_no_version(self):
        """BAD REQUEST is returned when patching without the version."""
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

        with open(os.environ['GOOGLE_APPLICATION_CREDENTIALS'], "r") as fh:
            owner_email = json.loads(fh.read())['client_email']

        expected_contents = {'contents': [col_file_item,
                                          col_ptr_item],
                             'description': 'd',
                             'details': {},
                             'name': 'n',
                             'owner': owner_email
                             }
        tests = [(dict(), None),
                 (dict(description="foo", name="cn"), dict(description="foo", name="cn")),
                 (dict(description="bar", details={1: 2}), dict(description="bar", details={'1': 2})),
                 (dict(add_contents=contents), None),  # Duplicates should be removed.
                 (dict(remove_contents=contents), dict(contents=[]))]
        for patch_payload, content_changes in tests:
            with self.subTest(patch_payload):
                res = self.app.patch("/v1/collections/{}".format(uuid),
                                     headers=get_auth_header(authorized=True),
                                     params=dict(version=version, replica="aws"),
                                     json=patch_payload)
                res.raise_for_status()
                self.assertNotEqual(version, res.json()["version"])
                version = res.json()["version"]
                res = self.app.get("/v1/collections/{}".format(uuid),
                                   headers=get_auth_header(authorized=True),
                                   params=dict(replica="aws", version=version))
                res.raise_for_status()
                collection = res.json()
                if content_changes:
                    expected_contents.update(content_changes)
                self.assertEqual(collection, expected_contents)

    def test_put_invalid_fragment(self):
        """PUT invalid fragment reference."""
        uuid = str(uuid4())
        self.addCleanup(self._delete_collection, uuid)
        res = self.app.put("/v1/collections",
                           headers=get_auth_header(authorized=True),
                           params=dict(uuid=uuid, version=datetime_to_version_format(datetime.now()), replica="aws"),
                           json=dict(name="n", description="d", details={}, contents=[self.invalid_ptr] * 128))
        self.assertEqual(res.status_code, requests.codes.unprocessable_entity)

    def test_patch_invalid_fragment(self):
        """PATCH invalid fragment reference."""
        res = self.app.patch("/v1/collections/{}".format(self.uuid),
                             headers=get_auth_header(authorized=True),
                             params=dict(version=self.version, replica="aws"),
                             json=dict(add_contents=[self.invalid_ptr] * 256))
        self.assertEqual(res.status_code, requests.codes.unprocessable_entity)

    def test_patch_excessive(self):
        """PATCH excess payload."""
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
             authorized: bool = True,
             uuid: typing.Optional[str] = None,
             version: typing.Optional[str] = None,
             replica: str = 'aws') -> typing.Tuple[str, str]:
        uuid = str(uuid4()) if uuid is None else uuid
        version = datetime_to_version_format(datetime.now()) if version is None else version

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

    def _delete_collection(self, uuid: str, replica: str = 'aws'):
        self.app.delete("/v1/collections/{}".format(uuid),
                        headers=get_auth_header(authorized=True),
                        params=dict(replica=replica))


if __name__ == '__main__':
    unittest.main()
