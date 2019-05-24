#!/usr/bin/env python
# coding: utf-8

"""
Functional Test of the API
"""
import base64
import json
import unittest
from itertools import combinations, product
from furl import furl
import os
import sys

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests.base_api_test import BaseAPITest
from tests.common import get_auth_header, service_accounts

class TestAuthentication(BaseAPITest, unittest.TestCase):
    def test_login(self):
        url = furl('/login')
        query_params = {
            'state': 'ABC',
            'redirect_uri': "http://localhost:8080"
        }
        url.add(query_params=query_params)
        resp = self.app.get(url.url)
        self.assertEqual(301, resp.status_code)
        self.assertEqual(resp.headers['Location'], '/oauth/authorize')

    def test_authorize(self):
        CLIENT_ID = "qtMgNk9fqVeclLtZl6WkbdJ59dP3WeAt"
        REDIRECT_URI = "http://localhost:8080"

        from uuid import uuid4
        states = [str(uuid4()), "1", "12", "ABCD", "\n\n"]
        scopes = ["openid", "email", "profile", "offline"]
        scopes_combination = [i for y in range(1, len(scopes) + 1) for i in combinations(scopes, y)]
        tests = product(states, scopes_combination)
        for state, scope in tests:
            _scope = ' '.join(scope)
            query_params = {
                "response_type": "code",
                "state": state,
                "redirect_uri": REDIRECT_URI,
                "scope": _scope
            }
            with self.subTest(f"with client_id: {state} {scope}"):  # TODO improve description
                url = furl("/oauth/authorize")
                url.add(query_params=query_params)
                url.add(query_params={"client_id": CLIENT_ID})

                resp = self.app.get(url.url)
                self.assertEqual(302, resp.status_code)
                redirect_url = furl(resp.headers['Location'])
                self.assertEqual(redirect_url.args["client_id"], CLIENT_ID)
                self.assertEqual(redirect_url.args["response_type"], 'code')
                self.assertEqual(redirect_url.args["state"], state)
                self.assertEqual(redirect_url.args["redirect_uri"], REDIRECT_URI)
                self.assertEqual(redirect_url.args["scope"], _scope)
                self.assertEqual(redirect_url.host, os.environ["OPENID_PROVIDER"])
                self.assertEqual(redirect_url.path, '/authorize')
            with self.subTest(f"without client_id: {state} {scope}"): #TODO improve description
                url = furl("/oauth/authorize")
                url.add(query_params=query_params)
                url.remove(query_params=["client_id"])

                resp = self.app.get(url.url)
                self.assertEqual(302, resp.status_code)
                redirect_url = furl(resp.headers['Location'])
                self.assertIn('client_id', redirect_url.args)
                self.assertEqual(redirect_url.args["response_type"], 'code')
                query_params["openid_provider"] = os.environ["OPENID_PROVIDER"]
                self.assertDictEqual(json.loads(base64.b64decode(redirect_url.args["state"])), query_params)
                redirect_uri = furl(redirect_url.args["redirect_uri"])
                self.assertTrue(redirect_uri.pathstr.endswith('/cb'))
                self.assertEqual(redirect_url.args["scope"], "openid email profile")
                self.assertEqual(redirect_url.host, os.environ["OPENID_PROVIDER"])
                self.assertEqual(redirect_url.path, '/authorize')

    def test_well_know_openid_configuration(self):
        expected_keys = ['issuer']
        expected_host = ['authorization_endpoint', 'token_endpoint', 'userinfo_endpoint', 'jwks_uri',
                         'revocation_endpoint']
        expected_response_types_supported = ['code']
        expected_supported_scopes = ['openid', 'profile', 'email']

        with self.subTest("openid configuration returned when host is provided in header."):
            host = os.environ['API_DOMAIN_NAME']
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

        if self.integration:
            with self.subTest("openid config is returned when no host is provided in the header"):
                resp = self.app.get('/.well-known/openid-configuration')
                self.assertEqual(200, resp.status_code)

            with self.subTest("Error return when invalid host is provided in header."):
                host = 'localhost:8080'
                resp = self.app.get('/.well-known/openid-configuration', headers={'host': host})
                self.assertEqual(403, resp.status_code)

    def test_serve_jwks_json(self):
        resp = self.app.get('/.well-known/jwks.json')
        body = json.loads(resp.body)
        self.assertIn('keys', body)
        self.assertEqual(200, resp.status_code)

    @unittest.skip("Not currently supported.")
    def test_revoke(self):
        with self.subTest("revoke denied when no token is included."):
            resp = self.app.get('/oauth/revoke')
            self.assertEqual(403, resp.status_code)  # TODO fix

    def test_userinfo(self):
        # TODO: login
        # TODO:use token to get userinfo
        tests =[
            {
                "headers": {},
                "expected_status_code": 401,
                "description": "userinfo denied when no token is included."
            }
        ]
        for test in tests:
            with self.subTest("POST " + test["description"]):
                resp = self.app.post('/oauth/userinfo', headers=test['headers'])
                self.assertEqual(test["expected_status_code"], resp.status_code)
            with self.subTest("GET " + test["description"]):
                resp = self.app.get('/oauth/userinfo', headers=test['headers'])
                self.assertEqual(test["expected_status_code"], resp.status_code)

    def test_serve_oauth_token(self):
        # TODO: login
        # TODO: get token
        with self.subTest("token denied when no query params provided."):
            resp = self.app.post('/oauth/token', headers={'Content-Type': "application/x-www-form-urlencoded"})
            self.assertEqual(401, resp.status_code)  # TODO fix

    def test_cb(self):
        resp = self.app.get('/internal/cb')
        self.assertEqual(400, resp.status_code)  # TODO fix


class TestApi(BaseAPITest, unittest.TestCase):
    def test_evaluate_policy(self):
        email = "test_evaluate_api@email.com"
        tests = [
            {
                'json_request_body': {
                    "action": ["dss:CreateSubscription"],
                    "resource": [f"arn:hca:dss:*:*:subscriptions/{email}/*"],
                    "principal": "test@email.com"
                },
                'response': {
                    'code': 200,
                    'result': False
                }
            },
            {
                'json_request_body': {
                    "action": ["fus:GetUser"],
                    "resource": [f"arn:hca:fus:*:*:user/{email}/policy"],
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
                headers.update(get_auth_header(service_accounts['admin']))
                resp = self.app.post('/v1/policies/evaluate', headers=headers, data=data)
                self.assertEqual(test['response']['code'], resp.status_code)
                self.assertEqual(test['response']['result'], json.loads(resp.body)['result'])

    def test_serve_swagger_ui(self):
        routes = ['/swagger.json', '/']
        for route in routes:
            with self.subTest(route):
                resp = self.app.get(route)
                resp.raise_for_status()

    def test_echo(self):
        body='Hello World!'
        resp = self.app.get('/echo', data=body)
        resp.raise_for_status()

    def test_version(self):
        resp = self.app.get('/internal/version')
        resp.raise_for_status()

    def test_health_check(self):
        resp = self.app.get('/internal/health')
        resp.raise_for_status()
        body = json.loads(resp.body)
        self.assertEqual(body['health_status'], 'ok')
        self.assertTrue(isinstance(body['services'], dict))


if __name__ == '__main__':
    unittest.main()