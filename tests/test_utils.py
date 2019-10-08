#!/usr/bin/env python

import io
import os
import sys
import unittest
from unittest import mock

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss import DSSException, DSSForbiddenException, Config
from dss.config import Replica
from dss.logging import configure_test_logging
from dss.storage.bundles import enumerate_available_bundles
from dss.storage.identifiers import TOMBSTONE_SUFFIX
from dss.util import UrlBuilder, security, multipart_parallel_upload
from dss.util.aws import ARN
from tests import UNAUTHORIZED_GCP_CREDENTIALS, get_service_jwt
from tests.infra import testmode


def setUpModule():
    configure_test_logging()


@testmode.standalone
class TestAwsUtils(unittest.TestCase):
    def test_aws_utils(self):
        arn = ARN(service="sns", resource="my_topic")
        arn.get_region()
        arn.get_account_id()
        str(arn)


@testmode.standalone
class TestUrlBuilder(unittest.TestCase):
    def test_simple(self):
        builder = UrlBuilder().set(
            scheme="https",
            netloc="humancellatlas.org",
            path="/abc",
            query=[
                ("ghi", "1"),
                ("ghi", "2"),
            ],
            fragment="def")
        self.assertEqual("https://humancellatlas.org/abc?ghi=1&ghi=2#def", str(builder))

    def test_has_query(self):
        builder = UrlBuilder().set(
            scheme="https",
            netloc="humancellatlas.org",
            path="/abc",
            query=[
                ("ghi", "1"),
                ("ghi", "2"),
            ],
            fragment="def")
        self.assertTrue(builder.has_query("ghi"))
        self.assertFalse(builder.has_query("abc"))

    def test_add_query(self):
        builder = UrlBuilder().set(
            scheme="https",
            netloc="humancellatlas.org",
            path="/abc",
            query=[
                ("ghi", "1"),
                ("ghi", "2"),
            ],
            fragment="def")
        self.assertTrue(builder.has_query("ghi"))
        self.assertFalse(builder.has_query("abc"))

        self.assertEqual("https://humancellatlas.org/abc?ghi=1&ghi=2#def", str(builder))

        builder.add_query("abc", "3")
        self.assertTrue(builder.has_query("ghi"))
        self.assertTrue(builder.has_query("abc"))

        self.assertEqual("https://humancellatlas.org/abc?ghi=1&ghi=2&abc=3#def", str(builder))

    def test_parse(self):
        builder = UrlBuilder("https://humancellatlas.org/abc?def=2#ghi")
        self.assertEqual("https://humancellatlas.org/abc?def=2#ghi", str(builder))

    def test_replace_query(self):
        builder = UrlBuilder("https://humancellatlas.org/abc?def=2#ghi")
        builder.replace_query("def", "4")
        self.assertEqual("https://humancellatlas.org/abc?def=4#ghi", str(builder))

    def test_replace_query_mulitple(self):
        builder = UrlBuilder("https://humancellatlas.org/abc?def=2&def=boo#ghi")
        builder.replace_query("def", "4")
        self.assertEqual("https://humancellatlas.org/abc?def=4#ghi", str(builder))


@testmode.standalone
class TestSecurity(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        dss.Config.set_config(dss.BucketConfig.TEST)

    @mock.patch('dss.Config._TRUSTED_GOOGLE_PROJECTS', new=['test.iam.gserviceaccount.com'])
    def test_authorized_issuer(self):
        valid_issuers = [{'iss': os.environ['OPENID_PROVIDER']},
                         {'iss': "travis-test@test.iam.gserviceaccount.com",
                          'sub': "travis-test@test.iam.gserviceaccount.com"}
                         ]
        for issuer in valid_issuers:
            with self.subTest(issuer):
                security.assert_authorized_issuer(issuer)

    def test_not_authorized_issuer(self):
        invalid_issuers = [{'iss': "https://project.auth0.com/"},
                           {'iss': "travis-test@test.iam.gserviceaccount.com",
                            'sub': "travis-test@test.iam.gserviceaccount.com"}
                           ]
        for issuer in invalid_issuers:
            with self.subTest(issuer):
                with self.assertRaises(DSSForbiddenException):
                    security.assert_authorized_issuer(issuer)

    def test_authorizated_group(self):
        valid_token_infos = [{"https://auth.data.humancellatlas.org/group": 'hca'},
                             {"https://auth.data.humancellatlas.org/group": 'public'}
                             ]
        for token_info in valid_token_infos:
            with self.subTest(token_info):
                security.assert_authorized_group(['hca', 'public'], token_info)

    def test_not_authorizated_group(self):
        invalid_token_info = [{'sub': "travis-test@human-cell-atlas-travis-test.gmail.com"},
                              {'sub': "travis-test@travis-test.iam.gserviceaccount.com.gmail.com"},
                              {"https://auth.data.humancellatlas.org/group": ''},
                              {"https://auth.data.humancellatlas.org/group": 'public'},
                              {"https://auth.data.humancellatlas.org/group": 'something_else'}
                              ]
        for token_info in invalid_token_info:
            with self.subTest(token_info):
                with self.assertRaises(DSSForbiddenException):
                    security.assert_authorized_group(['hca'], token_info)

    @mock.patch('dss.Config._OIDC_AUDIENCE', new=["https://dev.data.humancellatlas.org/",
                                                  "https://data.humancellatlas.org/"])
    @mock.patch('dss.Config._TRUSTED_GOOGLE_PROJECTS', new=['cool-project-188401.iam.gserviceaccount.com'])
    def test_verify_jwt_multiple_audience(self):
        jwts_positive = [
            get_service_jwt(UNAUTHORIZED_GCP_CREDENTIALS),
            get_service_jwt(UNAUTHORIZED_GCP_CREDENTIALS, audience="https://dev.data.humancellatlas.org/"),
            get_service_jwt(UNAUTHORIZED_GCP_CREDENTIALS, audience="https://data.humancellatlas.org/"),
            get_service_jwt(UNAUTHORIZED_GCP_CREDENTIALS, audience=["https://dev.data.humancellatlas.org/", "e"])
        ]
        jwt_negative = [
            get_service_jwt(UNAUTHORIZED_GCP_CREDENTIALS, audience="something else"),
            get_service_jwt(UNAUTHORIZED_GCP_CREDENTIALS, audience=["something", "e"])
        ]
        for jwt in jwts_positive:
            with self.subTest("Positive: " + jwt):
                security.verify_jwt(jwt)
        for jwt in jwt_negative:
            with self.subTest("Negative: " + jwt):
                self.assertRaises(dss.error.DSSException, security.verify_jwt, jwt)

    @mock.patch('dss.Config._OIDC_AUDIENCE', new="https://dev.data.humancellatlas.org/")
    @mock.patch('dss.Config._TRUSTED_GOOGLE_PROJECTS', new=['cool-project-188401.iam.gserviceaccount.com'])
    def test_verify_jwt_single_audience(self):
        jwts_positive = [
            get_service_jwt(UNAUTHORIZED_GCP_CREDENTIALS),
            get_service_jwt(UNAUTHORIZED_GCP_CREDENTIALS, audience="https://dev.data.humancellatlas.org/"),
            get_service_jwt(UNAUTHORIZED_GCP_CREDENTIALS, audience=["https://dev.data.humancellatlas.org/", "e"])
        ]
        jwt_negative = [
            get_service_jwt(UNAUTHORIZED_GCP_CREDENTIALS, audience="something else"),
            get_service_jwt(UNAUTHORIZED_GCP_CREDENTIALS, audience=["something", "e"])
        ]
        for jwt in jwts_positive:
            with self.subTest("Positive: " + jwt):
                security.verify_jwt(jwt)
        for jwt in jwt_negative:
            with self.subTest("Negative: " + jwt):
                self.assertRaises(dss.error.DSSException, security.verify_jwt, jwt)

    def test_negative_verify_jwt(self):
        jwts = [get_service_jwt(UNAUTHORIZED_GCP_CREDENTIALS)]
        for jwt in jwts:
            with self.subTest(jwt):
                with self.assertRaises(DSSException):
                    security.verify_jwt(jwt)

    def test_custom_email_claims(self):
        self.addCleanup(self.restore_email_claims, os.environ.pop('OIDC_EMAIL_CLAIM', 'EMPTY'))
        email = 'email@email.com'
        email_claim = 'email@claim.com'
        tests = [
            ({'email': email, Config.get_OIDC_email_claim(): email_claim}, email_claim),
            ({Config.get_OIDC_email_claim(): 'email@claim.com'}, email_claim),
            ({'email': email}, email)
        ]

        for param, result in tests:
            with self.subTest(f"no custom claim {param}"):
                self.assertEqual(security.get_token_email(param), result)

        os.environ['OIDC_EMAIL_CLAIM'] = 'TEST_CLAIM'
        for param, result in tests:
            with self.subTest(f"custom claim {param}"):
                self.assertEqual(security.get_token_email(param), result)

        with self.subTest("missing claim"):
            with self.assertRaises(DSSException) as ex:
                security.get_token_email({})
            self.assertEqual(ex.exception.status, 401)
            self.assertEqual(ex.exception.message, 'Authorization token is missing email claims.')

    def test_multipart_parallel_upload(self):
        data = os.urandom(7 * 1024 * 1024)
        metadata = {'something': "foolish"}
        part_size = 5 * 1024 * 1024
        s3_client = Config.get_native_handle(Replica.aws)
        bucket = os.environ['DSS_S3_BUCKET_TEST']
        with self.subTest("copy multiple parts"):
            with io.BytesIO(data) as fh:
                multipart_parallel_upload(
                    s3_client,
                    bucket,
                    "fake_key",
                    fh,
                    part_size=part_size,
                    metadata=metadata,
                    content_type="application/octet-stream",
                )
        part_size = 14 * 1024 * 1024
        with self.subTest("should work with single part"):
            with io.BytesIO(data) as fh:
                multipart_parallel_upload(
                    s3_client,
                    bucket,
                    "fake_key",
                    fh,
                    part_size=part_size,
                )

    @staticmethod
    def restore_email_claims(old):
        os.environ['OIDC_EMAIL_CLAIM'] = old

    class MockStorageHandler(object):
        bundle_list = ['bundles/00018f76-1e5d-4a6a-bbcd-8a261c10b121.2019-02-26T035448.489896Z',
                       'bundles/0007edde-f22c-4858-bf17-513dc2d05863.2019-02-26T033907.789762Z',
                       'bundles/0007edde-f22c-4858-bf17-513dc2d05863.2019-05-23T220340.829000Z',
                       'bundles/0007edde-f22c-4858-bf17-513dc2d05863.dead',
                       'bundles/001650e3-87a0-4145-9560-430930445071.2018-10-09T002318.558884Z',
                       'bundles/00169bc4-f1eb-415e-95d1-12b886550da9.2019-02-26T033907.789762Z',
                       'bundles/00171d5a-f14f-48b3-a467-0b0ac8208c6b.2019-02-26T033414.851898Z',
                       'bundles/00175253-e8f9-4f19-a070-1fcb25a57519.2018-10-18T203641.653105Z',
                       'bundles/00175253-e8f9-4f19-a070-1fcb25a57519.2018-10-18T203641.653105Z.dead',
                       'bundles/00176adc-f15d-4e46-8623-96cd047ca3df.2019-02-26T033558.843528Z',
                       'bundles/00190f26-f491-4d65-8bf4-bb7bd2b38313.2019-02-26T035253.735430Z',
                       'bundles/00193c56-85f3-4c72-b236-21e6359cb933.2019-02-26T035613.863196Z',
                       'bundles/001b92bb-2101-4e52-94d9-293fcb97ba32.2019-05-10T110312.514000Z',
                       'bundles/001b92bb-2101-4e52-94d9-293fcb97ba32.dead',
                       'bundles/001c3895-59ad-4897-967c-d6a33233f65e.2018-09-13T131642.918470Z',
                       'bundles/001d3eff-b102-4670-9dad-92c9fb6534bb.2019-02-26T033443.407780Z',
                       'bundles/001e784d-fbff-4ee4-aefd-6eb2c8cad783.2018-10-08T201847.591231Z']

        dead_bundles = ['bundles/0007edde-f22c-4858-bf17-513dc2d05863.2019-02-26T033907.789762Z',
                        'bundles/0007edde-f22c-4858-bf17-513dc2d05863.2019-05-23T220340.829000Z',
                        'bundles/0007edde-f22c-4858-bf17-513dc2d05863.dead',
                        'bundles/00175253-e8f9-4f19-a070-1fcb25a57519.2018-10-18T203641.653105Z',
                        'bundles/00175253-e8f9-4f19-a070-1fcb25a57519.2018-10-18T203641.653105Z.dead',
                        'bundles/001b92bb-2101-4e52-94d9-293fcb97ba32.2019-05-10T110312.514000Z',
                        'bundles/001b92bb-2101-4e52-94d9-293fcb97ba32.dead']

        def list_v2(self, *args, **kwargs):
            search_after = kwargs.get('start_after_key')
            bundle_list = self.bundle_list
            if search_after:
                idx = self.bundle_list.index(search_after)
                bundle_list = self.bundle_list[idx+1:-1]
            list_tuples = [(x, None) for x in bundle_list]
            return iter(list_tuples)

    @mock.patch("dss.Config.get_blobstore_handle")
    def test_uuid_enumeration(self, mock_list_v2):
        mock_list_v2.return_value = self.MockStorageHandler()
        resp = enumerate_available_bundles(replica='aws')
        for x in resp['bundles']:
            self.assertNotIn('.'.join([x['uuid'], x['version']]), self.MockStorageHandler.dead_bundles)

    @mock.patch("dss.Config.get_blobstore_handle")
    def test_tombstone_pages(self, mock_list_v2):
        test_per_page = [{"size": 5,
                          "last_good_bundle": {"uuid": "00176adc-f15d-4e46-8623-96cd047ca3df",
                                               "version": "2019-02-26T033558.843528Z"}},
                         {"size": 2,
                          "last_good_bundle": {"uuid": "001650e3-87a0-4145-9560-430930445071",
                                               "version": "2018-10-09T002318.558884Z"}},
                         {"size": 4,
                          "last_good_bundle": {"uuid": "00171d5a-f14f-48b3-a467-0b0ac8208c6b",
                                               "version": "2019-02-26T033414.851898Z"}}]

        mock_list_v2.return_value = self.MockStorageHandler()
        for tests in test_per_page:
            test_size = tests['size']
            last_good_bundle = tests['last_good_bundle']
            resp = enumerate_available_bundles(replica='aws', per_page=test_size)
            print(resp['bundles'])
            page_one = resp['bundles']
            for x in resp['bundles']:
                self.assertNotIn('.'.join([x['uuid'], x['version']]), self.MockStorageHandler.dead_bundles)
            self.assertDictEqual(last_good_bundle, resp['bundles'][-1])
            search_after = resp['search_after']
            resp = enumerate_available_bundles(replica='aws', per_page=2,
                                               search_after=search_after)
            print(resp['bundles'])
            for x in resp['bundles']:
                self.assertNotIn('.'.join([x['uuid'], x['version']]), self.MockStorageHandler.dead_bundles)
                self.assertNotIn(x, page_one)


if __name__ == '__main__':
    unittest.main()
