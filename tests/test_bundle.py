#!/usr/bin/env python
# coding: utf-8

import datetime
import hashlib
import io
import nestedcontext
import os
import requests
import sys
import threading
import time
import typing
import unittest
from unittest import mock
import urllib.parse
import uuid
import json
from requests.utils import parse_header_links
from urllib.parse import parse_qsl, urlparse, urlsplit
from functools import lru_cache

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from cloud_blobstore import BlobMetadataField
import dss
from dss.api.bundles import RETRY_AFTER_INTERVAL, bundle_file_id_metadata
from dss.config import BucketConfig, Config, override_bucket_config, Replica
from dss.util import UrlBuilder
from dss.storage.blobstore import test_object_exists
from dss.storage.hcablobstore import compose_blob_key
from dss.util.version import datetime_to_version_format
from dss.storage.bundles import get_bundle_manifest
from tests.infra import DSSAssertMixin, DSSUploadMixin, ExpectedErrorFields, get_env, testmode, TestAuthMixin
from tests.infra.server import ThreadedLocalServer, MockFusilladeHandler
from tests import eventually, get_auth_header


BUNDLE_GET_RETRY_COUNT = 60
"""For GET /bundles requests that require a retry, this is the maximum number of attempts we make."""


def setUpModule():
    Config.set_config(BucketConfig.TEST)
    MockFusilladeHandler.start_serving()


def tearDownModule():
    MockFusilladeHandler.stop_serving()


@testmode.standalone
class TestBundleApi(unittest.TestCase, TestAuthMixin, DSSAssertMixin, DSSUploadMixin):
    @classmethod
    def setUpClass(cls):
        cls.app = ThreadedLocalServer()
        cls.app.start()

    @classmethod
    def tearDownClass(cls):
        cls.app.shutdown()

    def setUp(self):
        dss.Config.set_config(dss.BucketConfig.TEST)
        self.s3_test_bucket = get_env("DSS_S3_BUCKET_TEST")
        self.gs_test_bucket = get_env("DSS_GS_BUCKET_TEST")
        self.s3_test_fixtures_bucket = get_env("DSS_S3_BUCKET_TEST_FIXTURES")
        self.gs_test_fixtures_bucket = get_env("DSS_GS_BUCKET_TEST_FIXTURES")

    def test_bundle_get(self):
        self._test_bundle_get(Replica.aws)
        self._test_bundle_get(Replica.gcp)

    def _test_bundle_get(self, replica: Replica):
        with self.subTest(replica):
            bundle_uuid = "011c7340-9b3c-4d62-bf49-090d79daf198"
            version = "2017-06-20T214506.766634Z"

            url = str(UrlBuilder()
                      .set(path="/v1/bundles/" + bundle_uuid)
                      .add_query("replica", replica.name)
                      .add_query("version", version))

            with override_bucket_config(BucketConfig.TEST_FIXTURE):
                resp_obj = self.assertGetResponse(
                    url,
                    requests.codes.ok,
                    headers=get_auth_header())

            self.assertEqual(resp_obj.json['bundle']['uuid'], bundle_uuid)
            self.assertEqual(resp_obj.json['bundle']['version'], version)
            self.assertEqual(resp_obj.json['bundle']['creator_uid'], 12345)
            self.assertEqual(resp_obj.json['bundle']['files'][0]['content-type'], "text/plain")
            self.assertEqual(resp_obj.json['bundle']['files'][0]['size'], 11358)
            self.assertEqual(resp_obj.json['bundle']['files'][0]['crc32c'], "e16e07b9")
            self.assertEqual(resp_obj.json['bundle']['files'][0]['name'], "LICENSE")
            self.assertEqual(resp_obj.json['bundle']['files'][0]['s3_etag'], "3b83ef96387f14655fc854ddc3c6bd57")
            self.assertEqual(resp_obj.json['bundle']['files'][0]['sha1'], "2b8b815229aa8a61e483fb4ba0588b8b6c491890")
            self.assertEqual(resp_obj.json['bundle']['files'][0]['sha256'],
                             "cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30")
            self.assertEqual(resp_obj.json['bundle']['files'][0]['uuid'], "ce55fd51-7833-469b-be0b-5da88ebebfcd")
            self.assertEqual(resp_obj.json['bundle']['files'][0]['version'], "2017-06-16T193604.240704Z")

    def test_bundle_paging(self):
        bundle_uuid = "7f8c686d-a439-4376-b367-ac93fc28df43"
        version = "2019-02-21T184000.899031Z"
        with override_bucket_config(BucketConfig.TEST_FIXTURE):
            handle = Config.get_blobstore_handle(Replica.aws)
            manifest = json.loads(handle.get(
                Replica.aws.bucket,
                f"bundles/{bundle_uuid}.{version}"
            ))
            expected_files = manifest['files']

        for replica in (Replica.aws, Replica.gcp):
            for pass_version in [True, False]:
                for per_page in [11, 33]:
                    with self.subTest(replica=replica, per_page=per_page, pass_version=pass_version):
                        self._test_bundle_get_paging(replica, expected_files, per_page, pass_version=pass_version)

            # This will get the entire manifest
            per_page = 500
            with self.subTest(replica=replica, per_page=per_page):
                self._test_bundle_get_paging(replica, expected_files, per_page, requests.codes.ok)

    def test_bundle_paging_too_small(self):
        """
        Should NOT be able to use a too-small per_page
        """
        for replica in (Replica.aws, Replica.gcp):
            with self.subTest(replica):
                self._test_bundle_get_paging(replica, list(), 9, codes=requests.codes.bad_request)

    def test_bundle_paging_too_large(self):
        """
        Should NOT be able to use a too-large per_page
        """
        for replica in (Replica.aws, Replica.gcp):
            with self.subTest(replica):
                self._test_bundle_get_paging(replica, list(), 501, codes=requests.codes.bad_request)

    def _test_bundle_get_paging(self,
                                replica,
                                expected_files: list,
                                per_page: int,
                                codes={requests.codes.ok, requests.codes.partial},
                                pass_version: bool = True):
        bundle_uuid = "7f8c686d-a439-4376-b367-ac93fc28df43"
        version = "2019-02-21T184000.899031Z"

        url_base = UrlBuilder().set(path="/v1/bundles/" + bundle_uuid)
        url_base.add_query("replica", replica.name)
        url_base.add_query("per_page", str(per_page))
        if pass_version:
            url_base.add_query("version", version)
        url = str(url_base)

        with override_bucket_config(BucketConfig.TEST_FIXTURE):
            resp_obj = self.assertGetResponse(url, codes, headers=get_auth_header())

        if not expected_files:
            return

        files: typing.List[dict] = list()
        files.extend(resp_obj.json['bundle']['files'])

        link_header = resp_obj.response.headers.get('Link')

        with override_bucket_config(BucketConfig.TEST_FIXTURE):
            while link_header:
                link = parse_header_links(link_header)[0]
                self.assertEquals(link['rel'], "next")
                parsed = urlsplit(link['url'])
                url = str(UrlBuilder().set(path=parsed.path, query=parse_qsl(parsed.query), fragment=parsed.fragment))
                self.assertIn("version", url)
                resp_obj = self.assertGetResponse(url, codes, headers=get_auth_header())
                files.extend(resp_obj.json['bundle']['files'])
                link_header = resp_obj.response.headers.get('Link')

                # Make sure we're getting the expected response status code
                self.assertEqual(resp_obj.response.headers['X-OpenAPI-Paginated-Content-Key'], 'bundle.files')
                if link_header:
                    self.assertEqual(resp_obj.response.headers['X-OpenAPI-Pagination'], 'true')
                    self.assertEqual(resp_obj.response.status_code, requests.codes.partial)
                else:
                    self.assertEqual(resp_obj.response.headers['X-OpenAPI-Pagination'], 'false')
                    self.assertEqual(resp_obj.response.status_code, requests.codes.ok)

        self.assertEquals(len(expected_files), len(files))

    def test_bundle_get_directaccess(self):
        self._test_bundle_get_directaccess(Replica.aws, True)
        self._test_bundle_get_directaccess(Replica.aws, False)
        self._test_bundle_get_directaccess(Replica.gcp, True)
        self._test_bundle_get_directaccess(Replica.gcp, False)

    def _test_bundle_get_directaccess(self, replica: Replica, explicit_version: bool):
        with self.subTest(f"{replica} {explicit_version}"):
            schema = replica.storage_schema

            bundle_uuid = "011c7340-9b3c-4d62-bf49-090d79daf198"
            version = "2017-06-20T214506.766634Z"

            url = UrlBuilder().set(path="/v1/bundles/" + bundle_uuid)
            url.add_query("replica", replica.name)
            url.add_query("directurls", "true")
            if explicit_version:
                url.add_query("version", version)

            with override_bucket_config(BucketConfig.TEST_FIXTURE):
                resp_obj = self.assertGetResponse(
                    str(url),
                    requests.codes.ok,
                    redirect_follow_retries=BUNDLE_GET_RETRY_COUNT,
                    min_retry_interval_header=RETRY_AFTER_INTERVAL,
                    override_retry_interval=1,
                    headers=get_auth_header()
                )

            directaccess_url = resp_obj.json['bundle']['files'][0]['url']
            splitted = urllib.parse.urlparse(directaccess_url)
            self.assertEqual(splitted.scheme, schema)
            bucket = splitted.netloc
            key = splitted.path[1:]  # ignore the / part of the path.

            handle = Config.get_blobstore_handle(replica)
            contents = handle.get(bucket, key)

            hasher = hashlib.sha1()
            hasher.update(contents)
            sha1 = hasher.hexdigest()
            self.assertEqual(bucket, replica.checkout_bucket)
            self.assertEqual(sha1, "2b8b815229aa8a61e483fb4ba0588b8b6c491890")

    def test_bundle_get_presigned(self):
        self._test_bundle_get_presigned(Replica.aws, True)
        self._test_bundle_get_presigned(Replica.aws, False)
        self._test_bundle_get_presigned(Replica.gcp, True)
        self._test_bundle_get_presigned(Replica.gcp, False)

    def _test_bundle_get_presigned(self, replica: Replica, explicit_version: bool):
        with self.subTest(f"{replica} {explicit_version}"):
            bundle_uuid = "011c7340-9b3c-4d62-bf49-090d79daf198"
            version = "2017-06-20T214506.766634Z"
            url = UrlBuilder().set(path="/v1/bundles/" + bundle_uuid)
            url.add_query("replica", replica.name)
            url.add_query("presignedurls", "true")
            if explicit_version:
                url.add_query("version", version)

            with override_bucket_config(BucketConfig.TEST_FIXTURE):
                resp_obj = self.assertGetResponse(
                    str(url),
                    requests.codes.ok,
                    redirect_follow_retries=BUNDLE_GET_RETRY_COUNT,
                    min_retry_interval_header=RETRY_AFTER_INTERVAL,
                    override_retry_interval=1,
                    headers=get_auth_header()
                )

            presigned_url = resp_obj.json['bundle']['files'][0]['url']
            resp = requests.get(presigned_url)
            contents = resp.content

            hasher = hashlib.sha1()
            hasher.update(contents)
            sha1 = hasher.hexdigest()
            self.assertEqual(sha1, "2b8b815229aa8a61e483fb4ba0588b8b6c491890")

    def test_bundle_get_directurl_and_presigned(self):
        self._test_bundle_get_directurl_and_presigned(Replica.aws)
        self._test_bundle_get_directurl_and_presigned(Replica.gcp)

    def _test_bundle_get_directurl_and_presigned(self, replica: Replica):
        with self.subTest(replica):
            bundle_uuid = "011c7340-9b3c-4d62-bf49-090d79daf198"
            version = "2017-06-20T214506.766634Z"

            url = str(UrlBuilder()
                      .set(path="/v1/bundles/" + bundle_uuid)
                      .add_query("replica", replica.name)
                      .add_query("version", version)
                      .add_query("directurls", "true")
                      .add_query("presignedurls", "true"))

            with override_bucket_config(BucketConfig.TEST_FIXTURE):
                resp_obj = self.assertGetResponse(
                    url,
                    requests.codes.bad_request,
                    headers=get_auth_header()
                )
                self.assertEqual(resp_obj.json['code'], "only_one_urltype")

    def test_bundle_get_deleted(self):
        bundle_uuid = "deadbeef-0000-4a6b-8f0d-a7d2105c23be"
        version = "2017-12-05T235850.950361Z"
        # whole bundle delete
        self._test_bundle_get_deleted(Replica.aws, bundle_uuid, version, None)
        self._test_bundle_get_deleted(Replica.gcp, bundle_uuid, version, None)
        # get latest undeleted version
        bundle_uuid = "deadbeef-0001-4a6b-8f0d-a7d2105c23be"
        expected_version = "2017-12-05T235728.441373Z"
        self._test_bundle_get_deleted(Replica.aws, bundle_uuid, None, expected_version)
        self._test_bundle_get_deleted(Replica.gcp, bundle_uuid, None, expected_version)
        # specific version delete
        version = "2017-12-05T235850.950361Z"
        self._test_bundle_get_deleted(Replica.aws, bundle_uuid, version, None)
        self._test_bundle_get_deleted(Replica.gcp, bundle_uuid, version, None)

    def _test_bundle_get_deleted(self,
                                 replica: Replica,
                                 bundle_uuid: str,
                                 version: typing.Optional[str],
                                 expected_version: typing.Optional[str]):
        with self.subTest(f"{replica} {bundle_uuid} {version} {expected_version}"):
            with override_bucket_config(BucketConfig.TEST_FIXTURE):
                bundle_metadata = get_bundle_manifest(
                    uuid=bundle_uuid,
                    replica=replica,
                    version=version,
                    bucket=None,
                )
                bundle_version = None if bundle_metadata is None else bundle_metadata['version']
            self.assertEquals(
                bundle_version,
                expected_version
            )

    def test_bundle_get_checkout(self):
        self._test_bundle_get_checkout(Replica.aws, self.s3_test_fixtures_bucket, self.s3_test_bucket)
        self._test_bundle_get_checkout(Replica.gcp, self.gs_test_fixtures_bucket, self.gs_test_bucket)

    def _test_bundle_get_checkout(self, replica: Replica, test_fixtures_bucket: str, test_bucket: str):
        schema = replica.storage_schema
        handle = Config.get_blobstore_handle(replica)

        # upload test bundle from test fixtures bucket
        bundle_uuid = str(uuid.uuid4())
        file_uuid_1 = str(uuid.uuid4())
        file_uuid_2 = str(uuid.uuid4())
        filenames = ["file_1", "file_2"]
        resp_obj_1 = self.upload_file_wait(
            f"{schema}://{test_fixtures_bucket}/test_good_source_data/0",
            replica,
            file_uuid_1,
            bundle_uuid=bundle_uuid,
        )
        resp_obj_2 = self.upload_file_wait(
            f"{schema}://{test_fixtures_bucket}/test_good_source_data/1",
            replica,
            file_uuid_2,
            bundle_uuid=bundle_uuid,
        )
        file_version_1 = resp_obj_1.json['version']
        file_version_2 = resp_obj_2.json['version']

        # generate blob keys
        file_metadata = json.loads(
            handle.get(
                test_bucket,
                f"files/{file_uuid_1}.{file_version_1}"
            ).decode("utf-8"))
        file_key_1 = compose_blob_key(file_metadata)
        file_metadata = json.loads(
            handle.get(
                test_bucket,
                f"files/{file_uuid_2}.{file_version_2}"
            ).decode("utf-8"))
        file_key_2 = compose_blob_key(file_metadata)

        bundle_version = datetime_to_version_format(datetime.datetime.utcnow())
        self.put_bundle(
            replica,
            bundle_uuid,
            [(file_uuid_1, file_version_1, filenames[0]), (file_uuid_2, file_version_2, filenames[1])],
            bundle_version,
        )

        url = str(UrlBuilder()
                  .set(path="/v1/bundles/" + bundle_uuid)
                  .add_query("replica", replica.name)
                  .add_query("version", bundle_version)
                  .add_query("presignedurls", "true"))

        @eventually(10, 2)
        def assert_creation_dates_updated(prev_creation_dates):
            creation_dates = list(blob[1] for blob in handle.list(replica.checkout_bucket, bundle_uuid))
            self.assertTrue(creation_dates[i] > prev_creation_dates[i] for i in range(len(creation_dates)))

        def force_checkout():
            handle.copy(test_bucket, file_key_1,
                        replica.checkout_bucket, f"bundles/{bundle_uuid}.{bundle_version}/file_1")
            handle.copy(test_bucket, file_key_2,
                        replica.checkout_bucket, f"bundles/{bundle_uuid}.{bundle_version}/file_2")

        with override_bucket_config(BucketConfig.TEST), \
                mock.patch("dss.storage.checkout.bundle.start_bundle_checkout") as mock_start_bundle_checkout, \
                mock.patch("dss.storage.checkout.bundle.get_bundle_checkout_status") as mock_get_bundle_checkout_status:
            mock_start_bundle_checkout.return_value = 1
            mock_get_bundle_checkout_status.return_value = {'status': "RUNNING"}
            with self.subTest(f"{replica}: Initiate checkout and return 301 if bundle has not been checked out"):
                # assert 301 redirect on first GET
                self.assertGetResponse(url, requests.codes.moved, redirect_follow_retries=0, headers=get_auth_header())
                mock_start_bundle_checkout.assert_called_once_with(replica,
                                                                   bundle_uuid,
                                                                   bundle_version,
                                                                   dst_bucket=replica.checkout_bucket)
                force_checkout()
                # assert 200 on subsequent GET
                self.assertGetResponse(url, requests.codes.ok, redirect_follow_retries=5, override_retry_interval=0.5,
                                       headers=get_auth_header())
                mock_start_bundle_checkout.reset_mock()

            with self.subTest(f"{replica}: Initiate checkout and return 301 if file is missing from checkout bundle"):
                handle.delete(replica.checkout_bucket, f"bundles/{bundle_uuid}.{bundle_version}/file_1")
                # assert 301 redirect on first GET
                self.assertGetResponse(url, requests.codes.moved, redirect_follow_retries=0, headers=get_auth_header())
                mock_start_bundle_checkout.assert_called_once_with(replica,
                                                                   bundle_uuid,
                                                                   bundle_version,
                                                                   dst_bucket=replica.checkout_bucket)
                force_checkout()
                # assert 200 on subsequent GET
                self.assertGetResponse(url, requests.codes.ok, redirect_follow_retries=5, override_retry_interval=0.5,
                                       headers=get_auth_header())
                mock_start_bundle_checkout.reset_mock()

            with self.subTest(f"{replica}: Initiate checkout and return 200 if a file in checkout bundle is stale"):
                now = datetime.datetime.now(datetime.timezone.utc)
                previous_creation_dates = list(blob[1] for blob in handle.list(replica.checkout_bucket, bundle_uuid))
                stale_creation_date = now - datetime.timedelta(days=int(os.environ['DSS_BLOB_PUBLIC_TTL_DAYS']),
                                                               hours=1,
                                                               minutes=5)
                with mock.patch("dss.storage.checkout.bundle._list_checkout_bundle") as mock_list_checkout_bundle:
                    mock_list_checkout_bundle.return_value = list(
                        ((f"bundles/{bundle_uuid}.{bundle_version}/{filename}",
                          {BlobMetadataField.CREATED: stale_creation_date if i == 1 else now})
                            for i, filename in enumerate(filenames))
                    )
                    self.assertGetResponse(url, requests.codes.ok, redirect_follow_retries=0, headers=get_auth_header())
                mock_start_bundle_checkout.assert_called_once_with(replica,
                                                                   bundle_uuid,
                                                                   bundle_version,
                                                                   dst_bucket=replica.checkout_bucket)
                force_checkout()
                assert_creation_dates_updated(previous_creation_dates)
                mock_start_bundle_checkout.reset_mock()

            with self.subTest(
                    f"{replica}: Initiate checkout and return 301 if a file in checkout bundle is nearly expired"):
                now = datetime.datetime.now(datetime.timezone.utc)
                near_expired_creation_date = now - datetime.timedelta(days=int(os.environ['DSS_BLOB_TTL_DAYS']),
                                                                      minutes=-10)

                get_listing_fn = ("cloud_blobstore.s3.S3PagedIter.get_listing_from_response"
                                  if replica.name == "aws"
                                  else "cloud_blobstore.gs.GSPagedIter.get_listing_from_response")
                with mock.patch(get_listing_fn) as mock_get_listing:
                    mock_get_listing.return_value = (
                        (f"bundles/{bundle_uuid}.{bundle_version}/{filename}",
                         {BlobMetadataField.CREATED: near_expired_creation_date if i == 0 else now})
                        for i, filename in enumerate(filenames)
                    )
                    self.assertGetResponse(url, requests.codes.moved, redirect_follow_retries=0,
                                           headers=get_auth_header())
                mock_start_bundle_checkout.assert_called_once_with(replica,
                                                                   bundle_uuid,
                                                                   bundle_version,
                                                                   dst_bucket=replica.checkout_bucket)
                force_checkout()
                self.assertGetResponse(url, requests.codes.ok, redirect_follow_retries=5, override_retry_interval=0.5,
                                       headers=get_auth_header())
                mock_start_bundle_checkout.reset_mock()

            handle.delete(test_bucket, f"bundles/{bundle_uuid}.{bundle_version}")
            handle.delete(replica.checkout_bucket, f"bundles/{bundle_uuid}.{bundle_version}")

    def test_bundle_put(self):
        tests = [(Replica.aws, self.s3_test_fixtures_bucket), (Replica.gcp, self.gs_test_fixtures_bucket)]
        for replica, bucket in tests:
            self._test_bundle_put(replica, bucket)

            bundle_version = datetime_to_version_format(datetime.datetime.utcnow())
            bundle_uuid = str(uuid.uuid4())
            builder = UrlBuilder().set(path="/v1/bundles/" + bundle_uuid).add_query("replica", replica.name)
            if bundle_version:
                builder.add_query("version", bundle_version)
            url = str(builder)
            self._test_auth_errors('put', url,
                                   skip_group_test=True,
                                   json_request_body=dict(
                                       files=[
                                           dict(
                                               uuid=str(uuid.uuid4()),
                                               version=datetime_to_version_format(datetime.datetime.utcnow()),
                                               name="LICENSE",
                                               indexed=False,
                                           )],
                                       creator_uid=12345,
                                   ))

    def _test_bundle_put(self, replica: Replica, fixtures_bucket: str):
        schema = replica.storage_schema
        bundle_uuid = str(uuid.uuid4())
        file_uuid = str(uuid.uuid4())
        missing_file_uuid = str(uuid.uuid4())
        resp_obj = self.upload_file_wait(
            f"{schema}://{fixtures_bucket}/test_good_source_data/0",
            replica,
            file_uuid,
            bundle_uuid=bundle_uuid,
        )
        file_version = resp_obj.json['version']

        with self.subTest(f'{replica}: first bundle.'):
            bundle_version = datetime_to_version_format(datetime.datetime.utcnow())
            self.put_bundle(
                replica,
                bundle_uuid,
                [(file_uuid, file_version, "LICENSE")],
                bundle_version,
            )

        with self.subTest(f'{replica}: should be able to do this twice (i.e. same payload, same UUIDs)'):
            self.put_bundle(
                replica,
                bundle_uuid,
                [(file_uuid, file_version, "LICENSE")],
                bundle_version,
                requests.codes.ok,
            )

        with self.subTest(f'{replica}:  manifest response testing'):
            bundle_version = datetime_to_version_format(datetime.datetime.utcnow())
            resp_obj = self.put_bundle(
                replica,
                bundle_uuid,
                [(file_uuid, file_version, "LICENSE")],
                bundle_version,
            )
            self.assertEqual(resp_obj.json['manifest']['creator_uid'], 12345)
            self.assertEqual(resp_obj.json['manifest']['version'], bundle_version)
            self.assertEqual(resp_obj.json['manifest']['files'][0]['name'], 'LICENSE')
            self.assertEqual(resp_obj.json['manifest']['files'][0]['indexed'], False)
            self.assertEqual(resp_obj.json['manifest']['files'][0]['version'], file_version)

        with self.subTest(f'{replica}: should *NOT* be able to do this twice with different payload.'):
            self.put_bundle(
                replica,
                bundle_uuid,
                [(file_uuid, file_version, "LICENSE1")],
                bundle_version,
                requests.codes.conflict,
            )

        with self.subTest(f'{replica}: should *NOT* be able to do this without bundle version.'):
            self.put_bundle(
                replica,
                bundle_uuid,
                [(file_uuid, file_version, "LICENSE")],
                expected_code=requests.codes.bad_request
            )

        with self.subTest(f'{replica}: put fails when the bundle contains a duplicated file name.'):
            with nestedcontext.bind(time_left=lambda: 0):
                bundle_version = datetime_to_version_format(datetime.datetime.utcnow())
                bundle_uuid2 = str(uuid.uuid4())
                file_uuid2 = str(uuid.uuid4())
                resp_obj2 = self.upload_file_wait(
                    f"{schema}://{fixtures_bucket}/test_good_source_data/0",
                    replica,
                    file_uuid2,
                    bundle_uuid=bundle_uuid2,
                )
                file_version2 = resp_obj2.json['version']
                resp = self.put_bundle(
                    replica,
                    bundle_uuid2,
                    [(file_uuid, file_version, "LICENSE"), (file_uuid2, file_version2, "LICENSE")],
                    bundle_version,
                    expected_code=requests.codes.bad_request
                )
                self.assertEqual(json.loads(resp.body)['code'], 'duplicate_filename')

        with self.subTest(f'{replica}: put succeeds when bundle contains multiple files with different names.'):
            with nestedcontext.bind(time_left=lambda: 0):
                bundle_version = datetime_to_version_format(datetime.datetime.utcnow())
                bundle_uuid3 = str(uuid.uuid4())
                resp = self.put_bundle(
                    replica,
                    bundle_uuid3,
                    [(file_uuid, file_version, "LICENSE"), (file_uuid, file_version, "LIasdfCENSE")],
                    bundle_version,
                    expected_code=requests.codes.created
                )

        with self.subTest(f'{replica}: put fails when an invalid bundle_uuid is supplied.'):
            bundle_version = datetime_to_version_format(datetime.datetime.utcnow())
            self.put_bundle(
                replica,
                "12345",
                [(file_uuid, file_version, "LICENSE")],
                bundle_version,
                expected_code=requests.codes.bad_request
            )

        with self.subTest(f'{replica}: put bundle fails when an invalid version is supplied'):
            self.put_bundle(
                replica,
                bundle_uuid,
                [(file_uuid, file_version, "LICENSE")],
                "ABCD",
                expected_code=requests.codes.bad_request
            )

        with self.subTest(f'{replica}: should *NOT* be able to upload a bundle with a missing file, but we should get '
                          'requests.codes.bad.'):
            with nestedcontext.bind(time_left=lambda: 0):
                bundle_version = datetime_to_version_format(datetime.datetime.utcnow())
                resp_obj = self.put_bundle(
                    replica,
                    bundle_uuid,
                    [
                        (file_uuid, file_version, "LICENSE0"),
                        (missing_file_uuid, file_version, "LICENSE1"),
                    ],
                    bundle_version,
                    expected_code=requests.codes.bad
                )
                self.assertEqual(resp_obj.json['code'], "file_missing")

        with self.subTest(f'{replica}: uploads a file, but delete the file metadata. put it back after a delay.'):
            self.upload_file_wait(
                f"{schema}://{fixtures_bucket}/test_good_source_data/0",
                replica,
                missing_file_uuid,
                file_version,
                bundle_uuid=bundle_uuid
            )
            handle = Config.get_blobstore_handle(replica)
            bucket = replica.bucket
            file_metadata = handle.get(bucket, f"files/{missing_file_uuid}.{file_version}")
            handle.delete(bucket, f"files/{missing_file_uuid}.{file_version}")

            class UploadThread(threading.Thread):
                def run(innerself):
                    time.sleep(5)
                    data_fh = io.BytesIO(file_metadata)
                    handle.upload_file_handle(bucket, f"files/{missing_file_uuid}.{file_version}", data_fh)

            # start the upload (on a delay...)
            upload_thread = UploadThread()
            upload_thread.start()

            # this should at first fail to find one of the files, but the UploadThread will eventually upload the file
            # metadata.  since we give the upload bundle process ample time to spin, it should eventually find the file
            # metadata and succeed.
            with nestedcontext.bind(time_left=lambda: sys.maxsize):
                bundle_version = datetime_to_version_format(datetime.datetime.utcnow())
                self.put_bundle(
                    replica,
                    bundle_uuid,
                    [
                        (file_uuid, file_version, "LICENSE0"),
                        (missing_file_uuid, file_version, "LICENSE1"),
                    ],
                    bundle_version,
                    expected_code=requests.codes.created,
                )

    def test_bundle_delete(self):
        tests = [
            (Replica.aws, self.s3_test_fixtures_bucket, True),
            (Replica.gcp, self.gs_test_fixtures_bucket, True),
            (Replica.aws, self.s3_test_fixtures_bucket, False),
            (Replica.gcp, self.gs_test_fixtures_bucket, False)
        ]
        for test in tests:
            with self.subTest(f"{test[0].name}, {test[2]}"):
                self._test_bundle_delete(*test)

    def test_bundle_delete_auth_errors(self):
        replicas = [Replica.aws, Replica.gcp]
        for replica in replicas:
            # make delete request
            bundle_uuid = str(uuid.uuid4())
            bundle_version = datetime_to_version_format(datetime.datetime.utcnow())
            url_builder = UrlBuilder().set(path="/v1/bundles/" + bundle_uuid).add_query('replica', replica.name)
            if bundle_version:
                url_builder = url_builder.add_query('version', bundle_version)
            url = str(url_builder)
            json_request_body = dict(reason="reason")
            json_request_body['version'] = bundle_version
            self._test_auth_errors('delete', url, json_request_body=json_request_body, skip_group_test=True)

    def _test_bundle_delete(self, replica: Replica, fixtures_bucket: str, authorized: bool):
        schema = replica.storage_schema

        # prep existing bundle
        bundle_uuid = str(uuid.uuid4())
        file_uuid = str(uuid.uuid4())
        resp_obj = self.upload_file_wait(
            f"{schema}://{fixtures_bucket}/test_good_source_data/0",
            replica,
            file_uuid,
            bundle_uuid=bundle_uuid,
        )
        file_version = resp_obj.json['version']

        bundle_version = datetime_to_version_format(datetime.datetime.utcnow())
        self.put_bundle(
            replica,
            bundle_uuid,
            [(file_uuid, file_version, "LICENSE")],
            bundle_version,
        )

        handle = Config.get_blobstore_handle(replica)
        bucket = replica.bucket

        self.delete_bundle(replica, bundle_uuid, authorized=authorized)
        tombstone_exists = test_object_exists(handle, bucket, f"bundles/{bundle_uuid}.dead")
        self.assertEquals(tombstone_exists, authorized)

        self.delete_bundle(replica, bundle_uuid, bundle_version, authorized=authorized)
        tombstone_exists = test_object_exists(handle, bucket, f"bundles/{bundle_uuid}.{bundle_version}.dead")
        self.assertEquals(tombstone_exists, authorized)

    def test_delete_nonexistent(self):
        nonexistent_uuid = str(uuid.uuid4())
        self.delete_bundle(Replica.aws, nonexistent_uuid, authorized=True, expected_code=404)

    def test_no_replica(self):
        """
        Verify we raise the correct error code when we provide no replica.
        """
        bundle_uuid = "ce55fd51-7833-469b-be0b-5da88ec0ffee"

        url = str(UrlBuilder().set(path="/v1/bundles/" + bundle_uuid))

        with override_bucket_config(BucketConfig.TEST_FIXTURE):
            self.assertPutResponse(
                url,
                requests.codes.bad_request,
                json_request_body=dict(
                    files=[],
                    creator_uid=12345,
                ),
                expected_error=ExpectedErrorFields(
                    code="illegal_arguments",
                    status=requests.codes.bad_request,
                    expect_stacktrace=True),
                headers=get_auth_header()
            )

    def test_no_files(self):
        """
        Verify we raise the correct error code when we do not provide the list of files.
        """
        bundle_uuid = "ce55fd51-7833-469b-be0b-5da88ec0ffee"

        url = str(UrlBuilder()
                  .set(path="/v1/bundles/" + bundle_uuid)
                  .add_query("replica", "aws"))

        with override_bucket_config(BucketConfig.TEST_FIXTURE):
            self.assertPutResponse(
                url,
                requests.codes.bad_request,
                json_request_body=dict(
                    creator_uid=12345,
                ),
                expected_error=ExpectedErrorFields(
                    code="illegal_arguments",
                    status=requests.codes.bad_request,
                    expect_stacktrace=True),
                headers=get_auth_header()
            )

    def test_bundle_get_not_found(self):
        """
        Verify that we return the correct error message when the bundle cannot be found.
        """
        self._test_bundle_get_not_found(Replica.aws)
        self._test_bundle_get_not_found(Replica.gcp)

    def _test_bundle_get_not_found(self, replica: Replica):
        with self.subTest(replica):
            bundle_uuid = str(uuid.uuid4())

            url = str(UrlBuilder()
                      .set(path="/v1/bundles/" + bundle_uuid)
                      .add_query("replica", replica.name))

            with override_bucket_config(BucketConfig.TEST_FIXTURE):
                self.assertGetResponse(
                    url,
                    requests.codes.not_found,
                    expected_error=ExpectedErrorFields(
                        code="not_found",
                        status=requests.codes.not_found),
                    headers=get_auth_header())

            version = "2017-06-16T193604.240704Z"
            url = str(UrlBuilder()
                      .set(path="/v1/bundles/" + bundle_uuid)
                      .add_query("replica", replica.name)
                      .add_query("version", version))

            with override_bucket_config(BucketConfig.TEST_FIXTURE):
                self.assertGetResponse(
                    url,
                    requests.codes.not_found,
                    expected_error=ExpectedErrorFields(
                        code="not_found",
                        status=requests.codes.not_found),
                    headers=get_auth_header())

    def put_bundle(
            self,
            replica: Replica,
            bundle_uuid: str,
            files: typing.Iterable[typing.Tuple[str, str, str]],
            bundle_version: typing.Optional[str] = None,
            expected_code: int = requests.codes.created):
        builder = UrlBuilder().set(path="/v1/bundles/" + bundle_uuid).add_query("replica", replica.name)
        if bundle_version:
            builder.add_query("version", bundle_version)
        url = str(builder)

        resp_obj = self.assertPutResponse(
            url,
            expected_code,
            json_request_body=dict(
                files=[
                    dict(
                        uuid=file_uuid,
                        version=file_version,
                        name=file_name,
                        indexed=False,
                    )
                    for file_uuid, file_version, file_name in files
                ],
                creator_uid=12345,
            ),
            headers=get_auth_header()
        )

        if 200 <= resp_obj.response.status_code < 300:
            self.assertHeaders(
                resp_obj.response,
                {
                    'content-type': "application/json",
                }
            )
            self.assertIn('version', resp_obj.json)
            self.assertIn('manifest', resp_obj.json)
        return resp_obj

    def delete_bundle(
            self,
            replica: Replica,
            bundle_uuid: str,
            bundle_version: typing.Optional[str] = None,
            authorized: bool = True,
            expected_code: typing.Optional[int] = None):
        # make delete request
        url_builder = UrlBuilder().set(path="/v1/bundles/" + bundle_uuid).add_query('replica', replica.name)
        if bundle_version:
            url_builder = url_builder.add_query('version', bundle_version)
        url = str(url_builder)

        json_request_body = dict(reason="reason")
        if bundle_version:
            json_request_body['version'] = bundle_version

        if not expected_code:
            expected_code = requests.codes.ok if authorized else requests.codes.forbidden

        # delete and check results
        return self.assertDeleteResponse(
            url,
            expected_code,
            json_request_body=json_request_body,
            headers=get_auth_header(authorized=authorized)
        )

    @lru_cache()
    def _put_bundle(self, replica=Replica.aws):
        bundle_uuid = str(uuid.uuid4())
        bundle_version = datetime_to_version_format(datetime.datetime.utcnow())
        self.put_bundle(replica, bundle_uuid, files=[], bundle_version=bundle_version)
        return bundle_uuid, bundle_version

    @lru_cache()
    def _put_file(self, replica=Replica.aws):
        file_uuid = str(uuid.uuid4())
        resp_obj = self.upload_file_wait(
            f"s3://{get_env('DSS_S3_BUCKET_TEST_FIXTURES')}/test_good_source_data/0",
            Replica.aws,
            file_uuid,
        )
        file_version = resp_obj.json['version']
        return file_uuid, file_version

    def _patch_files(self, number_of_files=10, replica=Replica.aws):
        file_uuid, file_version = self._put_file(replica)
        return [{'indexed': False,
                 'name': str(uuid.uuid4()),
                 'uuid': file_uuid,
                 'version': file_version} for _ in range(number_of_files)]

    def test_patch_no_version(self):
        "BAD REQUEST is returned when patching without the version."
        bundle_uuid, _ = self._put_bundle()
        res = self.app.patch("/v1/bundles/{}".format(bundle_uuid),
                             headers=get_auth_header(authorized=True),
                             params=dict(replica="aws"),
                             json=dict())
        self.assertEqual(res.status_code, requests.codes.bad_request)

    def test_patch(self):
        bundle_uuid, bundle_version = self._put_bundle()
        files = self._patch_files()
        tests = [
            (dict(add_files=files[2:]), files[2:]),
            (dict(add_files=files[:2], remove_files=files[-2:]), files[:-2]),
            (dict(), files[:-2]),
            (dict(remove_files=files), []),
            (dict(remove_files=files), []),
        ]
        for patch_payload, expected_files in tests:
            with self.subTest(patch_payload):
                res = self.app.patch(
                    f"/v1/bundles/{bundle_uuid}",
                    headers=get_auth_header(authorized=True),
                    params=dict(version=bundle_version, replica="aws"),
                    json=patch_payload
                )
                self.assertEqual(res.status_code, requests.codes.ok)
                bundle_version = res.json()['version']
                self.assertEqual(res.json()['uuid'], bundle_uuid)
                res = self.app.get(
                    f"/v1/bundles/{bundle_uuid}",
                    headers=get_auth_header(authorized=True),
                    params=dict(version=bundle_version, replica="aws"),
                )
                self.assertEqual(bundle_version, res.json()['bundle']['version'])
                self.assertEqual(
                    set([bundle_file_id_metadata(f) for f in expected_files]),
                    set([bundle_file_id_metadata(f) for f in res.json()['bundle']['files']])
                )

    def test_patch_excessive(self):
        bundle_uuid, bundle_version = self._put_bundle()
        files = self._patch_files(number_of_files=1001)
        with self.subTest("Bad request is returned when adding > 1000 files in a single request."):
            res = self.app.patch(
                f"/v1/bundles/{bundle_uuid}",
                headers=get_auth_header(authorized=True),
                params=dict(version=bundle_version, replica="aws"),
                json=dict(add_files=files),
            )
            self.assertEqual(res.status_code, requests.codes.bad_request)
        with self.subTest("BAD REQUEST is returned when removing > 1000 files in a single request."):
            res = self.app.patch(
                f"/v1/bundles/{bundle_uuid}",
                headers=get_auth_header(authorized=True),
                params=dict(version=bundle_version, replica="aws"),
                json=dict(remove_files=files),
            )
            self.assertEqual(res.status_code, requests.codes.bad_request)

    def test_patch_name_collision(self):
        "BAD REQUEST is returned for file name collisions."
        bundle_uuid, bundle_version = self._put_bundle()
        files = self._patch_files(1)
        # First patch should work
        res = self.app.patch(
            f"/v1/bundles/{bundle_uuid}",
            headers=get_auth_header(authorized=True),
            params=dict(version=bundle_version, replica="aws"),
            json=dict(add_files=files),
        )
        self.assertEqual(res.status_code, requests.codes.ok)
        bundle_version = res.json()['version']
        # should NOT be able to patch again with the same name
        res = self.app.patch(
            f"/v1/bundles/{bundle_uuid}",
            headers=get_auth_header(authorized=True),
            params=dict(version=bundle_version, replica="aws"),
            json=dict(add_files=files),
        )
        self.assertEqual(res.status_code, requests.codes.bad_request)

    def test_enumeration_bundles(self):
        bundle_uuid, bundle_version = self._put_bundle()
        res = self.app.get(f"/v1/bundles/all",
                           params=dict(version=bundle_version, replica="aws", per_page=10))
        self.assertIn(res.status_code, (requests.codes.okay, requests.codes.partial))
        if res.status_code is requests.codes.partial:
            body = res.json()
            page_one = body['bundles']
            link = urlparse(body['link'])
            formatted_link = f'{link.path}?{link.query}'
            res = self.app.get(formatted_link)
            self.assertIn(res.status_code, (requests.codes.okay, requests.codes.partial))
            for x in res.json()['bundles']:
                self.assertNotIn(x, page_one)


if __name__ == '__main__':
    unittest.main()
