#!/usr/bin/env python
# coding: utf-8

"""
Functional Test of the API
"""
import json
import os
import sys
import unittest

from furl import furl

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests import eventually
from tests.base_api_test import BaseAPITest
from tests.common import get_auth_header, service_accounts


class TestApi(BaseAPITest, unittest.TestCase):

    @eventually(10, 0.5)
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
        headers = {'Content-Type': "application/json"}
        headers.update(get_auth_header(service_accounts['admin']))

        for test in tests:
            with self.subTest(test['json_request_body']):
                data = json.dumps(test['json_request_body'])
                resp = self.app.post('/v1/policies/evaluate', headers=headers, data=data)
                self.assertEqual(test['response']['code'], resp.status_code)
                self.assertEqual(test['response']['result'], json.loads(resp.body)['result'])

        with self.subTest("User Disabled"):
            resp = self.app.put(furl(f"/v1/user/{email}",
                                     query_params={'user_id': email, 'status': 'disabled'}).url,
                                headers=headers)
            self.assertEqual(200, resp.status_code)
            resp = self.app.post('/v1/policies/evaluate', headers=headers,
                                 data=json.dumps(tests[1]['json_request_body']))
            self.assertEqual(200, resp.status_code)
            self.assertEqual(False, json.loads(resp.body)['result'])

    def test_serve_swagger_ui(self):
        routes = ['/swagger.json', '/']
        for route in routes:
            with self.subTest(route):
                resp = self.app.get(route)
                resp.raise_for_status()

    def test_echo(self):
        body = 'Hello World!'
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
