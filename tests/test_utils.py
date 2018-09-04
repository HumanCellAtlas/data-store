#!/usr/bin/env python

import os
import sys
import unittest
from unittest import mock

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss import DSSException, DSSForbiddenException
from dss.logging import configure_test_logging
from dss.util import UrlBuilder, security
from dss.util.aws import ARN
from tests import UNAUTHORIZED_GCP_CREDENTIALS, get_service_jwt
from tests.infra import testmode


def setUpModule():
    configure_test_logging()


class TestAwsUtils(unittest.TestCase):
    @testmode.standalone
    def test_aws_utils(self):
        arn = ARN(service="sns", resource="my_topic")
        arn.get_region()
        arn.get_account_id()
        str(arn)


class TestUrlBuilder(unittest.TestCase):
    @testmode.standalone
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

    @testmode.standalone
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

    @testmode.standalone
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

    @testmode.standalone
    def test_parse(self):
        builder = UrlBuilder("https://humancellatlas.org/abc?def=2#ghi")
        self.assertEqual("https://humancellatlas.org/abc?def=2#ghi", str(builder))

    @testmode.standalone
    def test_replace_query(self):
        builder = UrlBuilder("https://humancellatlas.org/abc?def=2#ghi")
        builder.replace_query("def", "4")
        self.assertEqual("https://humancellatlas.org/abc?def=4#ghi", str(builder))

    @testmode.standalone
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

    @mock.patch('dss.Config._OIDC_AUDIENCE', new="https://dev.data.humancellatlas.org/")
    @mock.patch('dss.Config._TRUSTED_GOOGLE_PROJECTS', new=['cool-project-188401.iam.gserviceaccount.com'])
    def test_verify_jwt(self):
        jwts = [get_service_jwt(UNAUTHORIZED_GCP_CREDENTIALS)]
        for jwt in jwts:
            with self.subTest(jwt):
                security.verify_jwt(jwt)

    def test_negative_verify_jwt(self):
        jwts = [get_service_jwt(UNAUTHORIZED_GCP_CREDENTIALS)]
        for jwt in jwts:
            with self.subTest(jwt):
                with self.assertRaises(DSSException):
                    security.verify_jwt(jwt)


if __name__ == '__main__':
    unittest.main()
