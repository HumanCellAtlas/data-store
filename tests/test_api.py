#!/usr/bin/env python
# coding: utf-8

"""
Functional Test of the API
"""

import os
import sys
import unittest
from furl import furl

import requests
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import WebApplicationClient

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

os.environ['OPENID_PROVIDER'] = "https://auth.dev.data.humancellatlas.org/"

from tests.infra.server import ThreadedLocalServer

class JWTClient(WebApplicationClient):
    def _add_bearer_token(self, uri, http_method='GET', body=None, headers=None, *args, **kwargs):
        headers["Authorization"] = "Bearer {}".format(self.token["id_token"])
        return uri, headers, body


def application_secrets(domain):
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
        cls.app = ThreadedLocalServer()
        cls.app.start()
        cls.domain = f"localhost:{cls.app._port}"
        cls.session = requests.Session()

    @classmethod
    def tearDownClass(cls):
        cls.app.shutdown()

    def test_login(self):
        resp = self.app.get('/login')
        self.assertEqual(resp.status_code, 301)

    def test_authorize(self):
        scopes = ["openid", "email", "offline_access"]  # Is offline_access needed for CLI
        CLIENT_ID = "qtMgNk9fqVeclLtZl6WkbdJ59dP3WeAt"
        CLIENT_SECRET = "JDE9KHBzrvNryDdzr3gNkyCMhXEUdMrzMcBrTXoRCNM0RlODP6NzlOxqF7Yx7O1F"
        REDIRECT_URIS = [
            "urn:ietf:wg:oauth:2.0:oob",
            "http://localhost:8080"
        ]

        from uuid import uuid4
        state = str(uuid4())
        url = furl("/authorize")
        url.add(query_params={"client_id": CLIENT_ID,
                              "response_type": "code",
                              "state": state,
                              "redirect_uri": REDIRECT_URIS,
                              "duration": "temporary",
                              "scope": scopes})

        with self.subTest("with client_id"):
            resp = self.app.get(url.url)
            self.assertEqual(resp.status_code, 302)

        with self.subTest("without client_id"):
            url.remove(query_params=["client_id"])
            resp = self.app.get(url.url)
            self.assertEqual(resp.status_code, 302)

    def test_well_know_openid_configuration(self):
        resp = self.app.get('/.well-known/openid-configuration', headers={'host':'auth.dev.data.humancellatlas.org'})
        resp.raise_for_status()

    def test_serve_jwks_json(self):
        resp = self.app.get('/.well-known/jwks.json')
        resp.raise_for_status()

    def test_revoke(self):
        resp = self.app.get('/oauth/revoke')
        resp.raise_for_status()

    def test_userinfo(self):
        resp = self.app.get('/userinfo')
        resp.raise_for_status()

    def test_serve_oauth_token(self):
        resp = self.app.post('/oauth/token')
        resp.raise_for_status()

    def test_echo(self):
        resp = self.app.get('/echo')
        resp.raise_for_status()

    def test_cb(self):
        resp = self.app.get('/cb')
        resp.raise_for_status()

    def test_evaluate_policy(self):
        resp = self.app.get('/policies/evaluate')
        resp.raise_for_status()

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
