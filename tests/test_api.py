#!/usr/bin/env python
# coding: utf-8

"""
Functional Test of the API
"""
import base64
import json
import os
import sys
import unittest
from furl import furl

from oauthlib.oauth2 import WebApplicationClient

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import fusillade
from fusillade.clouddirectory import cleanup_directory
from tests.common import random_hex_string, eventually

old_directory_name = os.getenv("FUSILLADE_DIR", None)
directory_name = "test_api_" + random_hex_string()

os.environ['OPENID_PROVIDER'] = "humancellatlas.auth0.com"
os.environ["FUSILLADE_DIR"] = directory_name

from tests.infra.server import ChaliceTestHarness
# ChaliceTestHarness must be imported after FUSILLADE_DIR has be set


@eventually(5,1, {fusillade.errors.FusilladeException})
def tearDownModule():
    from app import directory
    cleanup_directory(directory._dir_arn)
    if old_directory_name:
        os.environ["FUSILLADE_DIR"] = old_directory_name


class JWTClient(WebApplicationClient):
    def _add_bearer_token(self, uri, http_method='GET', body=None, headers=None, *args, **kwargs):
        headers["Authorization"] = "Bearer {}".format(self.token["id_token"])
        return uri, headers, body


def application_secrets(domain):
    """
    client_secret is a public secret from the auth0 secret for the HCA DCP CLI.
    
    :param domain:
    :return:
    """
    return {
        "installed": {
            "auth_uri": f"https://{domain}/authorize",
            "client_id": "qtMgNk9fqVeclLtZl6WkbdJ59dP3WeAt",
            "client_secret": "JDE9KHBzrvNryDdzr3gNkyCMhXEUdMrzMcBrTXoRCNM0RlODP6NzlOxqF7Yx7O1F",
            "redirect_uris": [
                "urn:ietf:wg:oauth:2.0:oob",
                "http://localhost:8080"
            ],
            "token_uri": f"https://{domain}/oauth/token"
        }
    }


class TestApi(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = ChaliceTestHarness()

    def test_login(self):
        resp = self.app.get('/login?state=ABC')
        self.assertEqual(resp.status_code, 301)
        self.assertEqual(resp.headers['Location'], '/authorize')

    def test_authorize(self):
        scopes = "openid email profile"  # Is offline_access needed for CLI
        CLIENT_ID = "qtMgNk9fqVeclLtZl6WkbdJ59dP3WeAt"
        REDIRECT_URI = "http://localhost:8080"

        from uuid import uuid4
        state = str(uuid4())
        query_params = {
            "response_type": "code",
            "state": state,
            "redirect_uri": REDIRECT_URI,
            "scope": scopes
        }
        url = furl("/authorize")
        url.add(query_params=query_params)
        url.add(query_params={"client_id": CLIENT_ID})

        with self.subTest("with client_id"):
            resp = self.app.get(url.url)
            self.assertEqual(resp.status_code, 302)
            redirect_url = furl(resp.headers['Location'])
            self.assertEqual(redirect_url.args["client_id"], CLIENT_ID)
            self.assertEqual(redirect_url.args["response_type"], 'code')
            self.assertEqual(redirect_url.args["state"], state)
            self.assertEqual(redirect_url.args["redirect_uri"], REDIRECT_URI)
            self.assertEqual(redirect_url.args["scope"], scopes)
            self.assertEqual(redirect_url.host, 'humancellatlas.auth0.com')
            self.assertEqual(redirect_url.path, '/authorize')
        with self.subTest("without client_id"):
            url.remove(query_params=["client_id"])
            resp = self.app.get(url.url)
            self.assertEqual(resp.status_code, 302)
            redirect_url = furl(resp.headers['Location'])
            self.assertIn('client_id', redirect_url.args)
            self.assertEqual(redirect_url.args["response_type"], 'code')
            query_params["openid_provider"] = "humancellatlas.auth0.com"
            self.assertDictEqual(json.loads(base64.b64decode(redirect_url.args["state"])), query_params)
            expected_redirect_uri = furl(scheme='https', host=os.environ['API_DOMAIN_NAME'], path='cb')
            self.assertEqual(redirect_url.args["redirect_uri"], str(expected_redirect_uri))
            self.assertEqual(redirect_url.args["scope"], scopes)
            self.assertEqual(redirect_url.host, 'humancellatlas.auth0.com')
            self.assertEqual(redirect_url.path, '/authorize')

    def test_well_know_openid_configuration(self):
        expected_keys = ['issuer']
        expected_host = ['authorization_endpoint', 'token_endpoint', 'userinfo_endpoint', 'jwks_uri',
                         'revocation_endpoint']
        expected_response_types_supported = ['code']
        expected_supported_scopes = ['openid', 'profile', 'email']

        with self.subTest("openid cponfiguration returned when host is provided in header."):
            host = 'localhost:8000'
            resp = self.app.get('/.well-known/openid-configuration', headers={'host': host})
            resp.raise_for_status()
            body = json.loads(resp.body)
            for key in expected_keys:
                self.assertIn(key, body)
            for key in expected_host:
                self.assertIn(host, body[key])
            for key in expected_supported_scopes:
                self.assertIn(key, body['scopes_supported'])
            for key in expected_response_types_supported:
                self.assertIn(key, body['response_types_supported'])

        with self.subTest("error when no host in header."):
            resp = self.app.get('/.well-known/openid-configuration')
            self.assertEqual(resp.status_code, 500)

    def test_serve_jwks_json(self):
        resp = self.app.get('/.well-known/jwks.json')
        body = json.loads(resp.body)
        self.assertIn('keys', body)
        self.assertEqual(resp.status_code, 200)

    def test_revoke(self):
        resp = self.app.get('/oauth/revoke')
        self.assertEqual(resp.status_code, 404)  # TODO fix

    def test_userinfo(self):
        resp = self.app.get('/userinfo')
        self.assertEqual(resp.status_code, 401)  # TODO fix

    def test_serve_oauth_token(self):
        resp = self.app.post('/oauth/token')
        self.assertEqual(resp.status_code, 415)  # TODO fix

    def test_echo(self):
        resp = self.app.get('/echo')
        resp.raise_for_status()

    def test_cb(self):
        resp = self.app.get('/cb')
        self.assertEqual(resp.status_code, 500)  # TODO fix

    def test_evaluate_policy(self):
        email = "test@email.com"
        tests = [
            {
                'json_request_body': {
                    "action": "dss:CreateSubscription",
                    "resource": f"arn:hca:dss:*:*:subscriptions/{email}/*",
                    "principal": "test@email.com"
                },
                'response': {
                    'code': 200,
                    'result': False
                }
            },
            {
                'json_request_body': {
                    "action": "fus:GetUser",
                    "resource": f"arn:hca:fus:*:*:user/{email}/policy",
                    "principal": email
                },
                'response': {
                    'code': 200,
                    'result': True
                }
            }
        ]
        for test in tests:
            with self.subTest(test['json_request_body']):
                data=json.dumps(test['json_request_body'])
                headers={'Content-Type': "application/json"}
                resp = self.app.post('/policies/evaluate', headers=headers, data=data)
                self.assertEqual(test['response']['code'], resp.status_code)  # TODO fix
                self.assertEqual(test['response']['result'], json.loads(resp.body)['result'])

    def test_put_user(self):
        pass

    def test_get_user(self):
        pass

    def test_put_group(self):
        pass

    def test_get_group(self):
        pass

    def test_serve_swagger_ui(self):
        routes = ['/swagger.json', '/']
        for route in routes:
            with self.subTest(route):
                resp = self.app.get(route)
                resp.raise_for_status()


if __name__ == '__main__':
    unittest.main()
