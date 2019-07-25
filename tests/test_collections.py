#!/usr/bin/env python3
import typing
import itertools
import os
import sys
import unittest
import io
import json
import boto3
import logging

from uuid import uuid4
from datetime import datetime
from requests.utils import parse_header_links
from botocore.vendored import requests
from dcplib.s3_multipart import get_s3_multipart_chunk_size
from urllib.parse import parse_qsl, urlsplit
from google.cloud import storage as gs_storage

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests import get_auth_header, eventually
from tests.infra import generate_test_key, get_env, DSSAssertMixin, DSSUploadMixin, testmode
from tests.infra.server import ThreadedLocalServer
from tests.fixtures.cloud_uploader import ChecksummingSink
from dss.util.version import datetime_to_version_format
from dss.util import UrlBuilder
from dss.collections import owner_lookup
from dss.dynamodb import DynamoDBItemNotFound


logger = logging.getLogger(__name__)


@testmode.integration
class TestCollections(unittest.TestCase, DSSAssertMixin, DSSUploadMixin):
    @classmethod
    def setUpClass(cls):
        cls.app = ThreadedLocalServer()
        cls.app.start()

        cls.s3_file_uuid, cls.s3_file_version = cls.upload_file(cls.app, {"foo": 1}, replica='aws')
        cls.s3_col_file_item = dict(type="file", uuid=cls.s3_file_uuid, version=cls.s3_file_version)
        cls.s3_col_ptr_item = dict(type="foo", uuid=cls.s3_file_uuid, version=cls.s3_file_version, fragment="/foo")

        cls.gs_file_uuid, cls.gs_file_version = cls.upload_file(cls.app, {"foo": 1}, replica='gcp')
        cls.gs_col_file_item = dict(type="file", uuid=cls.gs_file_uuid, version=cls.gs_file_version)
        cls.gs_col_ptr_item = dict(type="foo", uuid=cls.gs_file_uuid, version=cls.gs_file_version, fragment="/foo")

        cls.contents = [cls.s3_col_file_item] * 8 + [cls.s3_col_ptr_item] * 8
        cls.uuid, cls.version = cls._put(cls, cls.contents)
        cls.invalid_ptr = dict(type="foo", uuid=cls.s3_file_uuid, version=cls.s3_file_version, fragment="/xyz")
        cls.paging_test_replicas = ('aws', 'gcp')

        with open(os.environ['GOOGLE_APPLICATION_CREDENTIALS'], 'r') as fh:
            cls.owner_email = json.loads(fh.read())['client_email']

    @classmethod
    def teardownClass(cls):
        cls._delete_collection(cls, cls.uuid)
        cls.app.shutdown()

    @staticmethod
    def upload_file(app, contents, replica):
        src_key = generate_test_key()
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

            if replica == 'gcp':
                gs_test_bucket = get_env("DSS_GS_BUCKET_TEST")
                gcp_client = gs_storage.Client.from_service_account_json(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
                gs_bucket = gcp_client.bucket(gs_test_bucket)
                blob = gs_bucket.blob(src_key)
                blob.upload_from_file(fh, content_type="application/json")
                blob.metadata = metadata
                blob.patch()
                source_url = f"gs://{gs_test_bucket}/{src_key}"

            if replica == 'aws':
                # TODO: consider switching to unmanaged uploader (putobject w/blob)
                s3_test_bucket = get_env("DSS_S3_BUCKET_TEST")
                s3 = boto3.resource('s3')
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

    def create_temp_user_collections(self, num: int, replica: str):
        if replica == 'aws':
            contents = [self.s3_col_file_item, self.s3_col_ptr_item]
        if replica == 'gcp':
            contents = [self.gs_col_file_item, self.gs_col_ptr_item]

        for i in range(num):
            uuid, _ = self._put(contents, replica=replica)
            self.addCleanup(self._delete_collection, uuid, replica)

    def fetch_collection_paging_response(self, codes, replica: str, per_page: int):
        """
        GET /collections and iterate through the paging responses containing all of a user's collections.

        If fetch_all is not True, this will return as soon as it gets one successful 206 paging reply.
        """
        url = UrlBuilder().set(path="/v1/collections/")
        url.add_query("replica", replica)
        url.add_query("per_page", str(per_page))
        resp_obj = self.assertGetResponse(str(url), codes, headers=get_auth_header(authorized=True))

        if codes == requests.codes.bad_request:
            return True

        link_header = resp_obj.response.headers.get('Link')
        paging_response = False

        while link_header:
            # Make sure we're getting the expected response status code
            self.assertEqual(resp_obj.response.status_code, requests.codes.partial)
            paging_response = True
            link = parse_header_links(link_header)[0]
            self.assertEquals(link['rel'], 'next')
            parsed = urlsplit(link['url'])
            url = UrlBuilder().set(path=parsed.path, query=parse_qsl(parsed.query), fragment=parsed.fragment)
            resp_obj = self.assertGetResponse(str(url),
                                              expected_code=codes,
                                              headers=get_auth_header(authorized=True))
            link_header = resp_obj.response.headers.get('Link')

        self.assertEqual(resp_obj.response.status_code, requests.codes.ok)
        return paging_response

    @eventually(timeout=5, interval=1, errors={ValueError})
    def check_collection_not_found(self, uuid):
        with self.assertRaises(DynamoDBItemNotFound):
            owner_lookup.get_collection(owner=self.owner_email, collection_fqid=uuid)

    def test_collection_paging(self):
        min_page = 10
        codes = {requests.codes.ok, requests.codes.partial}
        for replica in self.paging_test_replicas:
            self.create_temp_user_collections(num=min_page + 1, replica=replica)  # guarantee at least 1 paging response
            for per_page in [min_page, 100, 500]:
                with self.subTest(replica=replica, per_page=per_page):
                    paging_response = self.fetch_collection_paging_response(codes=codes,
                                                                            replica=replica,
                                                                            per_page=per_page)
                    if per_page == min_page:
                        self.assertTrue(paging_response)

    def test_collections_db(self):
        """Test that the dynamoDB functions work for a collection."""
        fake_uuid = str(uuid4())

        with self.subTest("Assert uuid is not already among the user's collections."):
            for value in owner_lookup.get_collection_fqids_for_owner(owner=self.owner_email):
                self.assertNotEqual(fake_uuid, value)

        with self.subTest("Test dynamoDB put_collection."):
            owner_lookup.put_collection(owner=self.owner_email, collection_fqid=fake_uuid)

        with self.subTest("Test dynamoDB get_collections_for_owner finds the put collection."):
            found = False
            for value in owner_lookup.get_collection_fqids_for_owner(owner=self.owner_email):
                if fake_uuid == value:
                    found = True
                    break
            self.assertEqual(found, True)

        with self.subTest("Test dynamoDB get_collection successfully finds collection."):
            owner_lookup.get_collection(owner=self.owner_email, collection_fqid=fake_uuid)

        with self.subTest("Test dynamoDB delete_collection."):
            owner_lookup.delete_collection(owner=self.owner_email, collection_fqid=fake_uuid)
            self.check_collection_not_found(fake_uuid)

        with self.subTest("Test dynamoDB delete_collection silently deletes now non-existent item."):
            owner_lookup.delete_collection(owner=self.owner_email, collection_fqid=fake_uuid)

        with self.subTest("Test dynamoDB put_collection (2 versions)."):
            owner_lookup.put_collection(owner=self.owner_email, collection_fqid=fake_uuid + '.v1')
            owner_lookup.put_collection(owner=self.owner_email, collection_fqid=fake_uuid + '.v2')
            versions = 0
            for value in owner_lookup.get_collection_fqids_for_owner(owner=self.owner_email):
                if value.startswith(fake_uuid):
                    versions += 1
            self.assertEqual(versions, 2)

        with self.subTest("Test dynamoDB delete_collection uuid (test 2 versions get deleted using one uuid)."):
            owner_lookup.delete_collection_uuid(owner=self.owner_email, uuid=fake_uuid)
            for value in owner_lookup.get_collection_fqids_for_owner(owner=self.owner_email):
                if value.startswith(fake_uuid):
                    raise ValueError(f'{fake_uuid} was removed from db, but {value} was found.')

        with self.subTest("Test dynamoDB get_collection does not find deleted versions."):
            self.check_collection_not_found(fake_uuid + '.v1')
            self.check_collection_not_found(fake_uuid + '.v2')

    def test_collection_paging_too_small(self):
        """Should NOT be able to use a too-small per_page."""
        for replica in self.paging_test_replicas:
            with self.subTest(replica):
                self.fetch_collection_paging_response(replica=replica, per_page=9, codes=requests.codes.bad_request)

    def test_collection_paging_too_large(self):
        """Should NOT be able to use a too-large per_page."""
        for replica in self.paging_test_replicas:
            with self.subTest(replica):
                self.fetch_collection_paging_response(replica=replica, per_page=501, codes=requests.codes.bad_request)

    @testmode.standalone
    def test_get(self):
        """GET a list of all collections belonging to the user."""
        res = self.app.get('/v1/collections',
                           headers=get_auth_header(authorized=True),
                           params=dict())
        res.raise_for_status()
        self.assertIn('collections', res.json())

    def test_put(self):
        """PUT new collection."""
        with self.subTest("with unique contents"):
            contents = [self.s3_col_file_item, self.s3_col_ptr_item]
            uuid, _ = self._put(contents)
            self.addCleanup(self._delete_collection, uuid)
            res = self.app.get("/v1/collections/{}".format(self.uuid),
                               headers=get_auth_header(authorized=True),
                               params=dict(replica="aws"))
            self.assertEqual(res.json()["contents"], [self.s3_col_file_item, self.s3_col_ptr_item])

        with self.subTest("with duplicated contents."):
            uuid, _ = self._put(self.contents)
            self.addCleanup(self._delete_collection, uuid)
            res = self.app.get("/v1/collections/{}".format(self.uuid),
                               headers=get_auth_header(authorized=True),
                               params=dict(replica="aws"))
            self.assertEqual(res.json()["contents"], [self.s3_col_file_item, self.s3_col_ptr_item])

    def test_get_uuid(self):
        """GET created collection."""
        res = self.app.get("/v1/collections/{}".format(self.uuid),
                           headers=get_auth_header(authorized=True),
                           params=dict(version=self.version, replica="aws"))
        res.raise_for_status()
        self.assertEqual(res.json()["contents"], [self.s3_col_file_item, self.s3_col_ptr_item])

    def test_get_uuid_latest(self):
        """GET latest version of collection."""
        res = self.app.get("/v1/collections/{}".format(self.uuid),
                           headers=get_auth_header(authorized=True),
                           params=dict(replica="aws"))
        res.raise_for_status()
        self.assertEqual(res.json()["contents"], [self.s3_col_file_item, self.s3_col_ptr_item])

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
        col_file_item = dict(type="file", uuid=self.s3_file_uuid, version=self.s3_file_version)
        col_ptr_item = dict(type="foo", uuid=self.s3_file_uuid, version=self.s3_file_version, fragment="/foo")
        contents = [col_file_item] * 8 + [col_ptr_item] * 8
        uuid, version = self._put(contents, replica='aws')
        self.addCleanup(self._delete_collection, uuid, replica='aws')

        expected_contents = {'contents': [col_file_item,
                                          col_ptr_item],
                             'description': 'd',
                             'details': {},
                             'name': 'n',
                             'owner': self.owner_email
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
                             json=dict(add_contents=[self.s3_col_ptr_item] * 1024))
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

    def test_put_excessive(self):
        """Test trying to PUT a collection with more than 1k items"""
        contents = [self.s3_col_ptr_item] * 1001  # the limit is 1000
        res = self.app.put('/v1/collections',
                           headers=get_auth_header(authorized=True),
                           params=dict(version=self.version, replica='aws', uuid=uuid4()),
                           json=dict(name="n", description="d", details={}, contents=contents))
        self.assertEqual(res.status_code, requests.codes.bad_request)

    def _put(self, contents: typing.List,
             authorized: bool = True,
             uuid: typing.Optional[str] = None,
             version: typing.Optional[str] = None,
             replica: str = 'aws') -> typing.Tuple[str, str]:
        uuid = str(uuid4()) if uuid is None else uuid
        version = datetime_to_version_format(datetime.now()) if version is None else version

        params = dict()
        if uuid != 'missing':
            params['uuid'] = uuid
        if version != 'missing':
            params['version'] = version
        if replica != 'missing':
            params['replica'] = replica

        res = self.app.put("/v1/collections",
                           headers=get_auth_header(authorized=authorized),
                           params=params,
                           json=dict(name="n", description="d", details={}, contents=contents))
        return res.json()["uuid"], res.json()["version"]

    def _delete_collection(self, uuid: str, replica: str = 'aws'):
        self.app.delete("/v1/collections/{}".format(uuid),
                        headers=get_auth_header(authorized=True),
                        params=dict(replica=replica))


if __name__ == '__main__':
    unittest.main()
