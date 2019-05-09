#!/usr/bin/env python
# coding: utf-8

"""
Functional Test of the Roles API
"""
import json
import os
import sys
import unittest
from furl import furl, quote

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests import random_hex_string, eventually

directory_name = "test_api_" + random_hex_string()
os.environ['OPENID_PROVIDER'] = "humancellatlas.auth0.com"
old_directory_name = os.getenv("FUSILLADE_DIR", None)
os.environ["FUSILLADE_DIR"] = directory_name


from tests.common import get_auth_header, service_accounts, create_test_statement
import fusillade
from fusillade import directory, User
from fusillade.clouddirectory import cleanup_directory, Role

from tests.infra.server import ChaliceTestHarness
# ChaliceTestHarness must be imported after FUSILLADE_DIR has be set


def setUpModule():
    User.provision_user(directory, service_accounts['admin']['client_email'], roles=['admin'])


@eventually(5,1, {fusillade.errors.FusilladeException})
def tearDownModule():
    cleanup_directory(directory._dir_arn)
    if old_directory_name:
        os.environ["FUSILLADE_DIR"] = old_directory_name


class TestRoleApi(unittest.TestCase):
    test_postive_names = [
        ('helloworl12345', "alpha numerica characters"),
         ('hello@world.com', "email format") ,
        ('hello-world=_@.,ZDc', "special characters"),
        ('HellOWoRLd', "different cases"),
        ('ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789'
        'ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789', "== 128 characters"),
        ("1", "one character")
                          ]
    test_negative_names = [
        ("&^#$Hello", "illegal characters 1"),
        ("! <>?world", "illegal characters 2"),
        ('ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789'
        'ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF01234567890', "> 128 characters"),
        ('', "empty")
    ]

    @classmethod
    def setUpClass(cls):
        cls.app = ChaliceTestHarness()

    def tearDown(self):
        directory.clear(
            users=[
                service_accounts['admin']['client_email'],
                service_accounts['user']['client_email']
            ])

    def test_positive(self):
        """
        Test Create Retrieve and Update

        1. Create a role
        2. retrieve that role
        3. modify that role
        4. retrieve modified role
        """
        role_id = 'test_positive'
        policy = create_test_statement(role_id)
        headers = {'Content-Type': "application/json"}
        headers.update(get_auth_header(service_accounts['admin']))

        url = furl('/v1/roles')
        data = json.dumps({
            'name': role_id,
            'policy': policy
        })
        resp = self.app.put(url.url, data=data, headers=headers)
        self.assertEqual(201, resp.status_code)

        url = furl(f'/v1/roles/{role_id}')
        resp = self.app.get(url.url, headers=headers)
        self.assertEqual(200, resp.status_code)
        expected_body = {
            'name': role_id,
            'policy': policy
        }
        self.assertEqual(expected_body, json.loads(resp.body))

        url = furl(f'/v1/roles/{role_id}/policy')
        policy = create_test_statement('ABCD')
        data = json.dumps({
            'policy': policy
        })
        resp = self.app.put(url.url, data=data, headers=headers)
        self.assertEqual(200, resp.status_code)

        url = furl(f'/v1/roles/{role_id}')
        resp = self.app.get(url.url, headers=headers)
        self.assertEqual(200, resp.status_code)
        expected_body = {
            'name': role_id,
            'policy': policy
        }
        self.assertEqual(expected_body, json.loads(resp.body))

    def test_put_role(self):
        url = furl('/v1/roles')
        data = json.dumps({
            'name': 'test_put_role',
            'policy': create_test_statement("test_role")
        })
        admin_auth_header = get_auth_header(service_accounts['admin'])
        tests = [
            {
                'name': '401 return when no auth headers.',
                'data': data,
                'headers': {},
                'expected_resp': 401
            },
            {
                'name': '403 return when unauthorized user.',
                'data': data,
                'headers': get_auth_header(service_accounts['user']),
                'expected_resp': 403
            },
            {
                'name': '201 returned when user is authorized.',
                'data': data,
                'headers': admin_auth_header,
                'expected_resp': 201
            },
            {
                'name': '409 returned when role already exists.',
                'data': data,
                'headers': admin_auth_header,
                'expected_resp': 409
            },
            {
                'name': '400 returned when an invalid policy is used',
                'data': json.dumps({
                    'name': 'test_role2',
                    'policy': 'garbage statement'
                }),
                'headers': admin_auth_header,
                'expected_resp': 400
            },
            {
                'name': '400 returned when creating a role with no name.',
                'data': json.dumps({
                    'policy': create_test_statement("test_role")
                }),
                'headers': admin_auth_header,
                'expected_resp': 400
            },
            {
                'name': '400 returned when creating a role with no policy.',
                'data': json.dumps({
                    'name': 'abcd',
                }),
                'headers': admin_auth_header,
                'expected_resp': 400
            }
        ]
        tests.extend([
            {
                'name': f'201 returned when creating a role when name is {description}',
                'data': json.dumps({
                    'name': name,
                    'policy': create_test_statement("test_role")
                }),
                'headers': admin_auth_header,
                'expected_resp': 201
            } for name, description in self.test_postive_names
        ])
        tests.extend([
            {
                'name': f'400 returned when creating a role when name is {description}',
                'data': json.dumps({
                    'name': name,
                    'policy': create_test_statement("test_role")
                }),
                'headers': admin_auth_header,
                'expected_resp': 400
            } for name, description in self.test_negative_names
        ])
        for test in tests:
            with self.subTest(test['name']):
                headers = {'Content-Type': "application/json"}
                headers.update(test['headers'])
                resp = self.app.put(url.url, data=test['data'], headers=headers)
                self.assertEqual(test['expected_resp'], resp.status_code)

    def test_get_role_id(self):
        role_id = 'test_get_role_id'
        admin_auth_header = get_auth_header(service_accounts['admin'])
        tests = [
            {
                'name': '401 return when no auth headers.',
                'headers': {},
                'role_id': role_id,
                'expected_resp': 401
            },
            {
                'name': '403 return when unauthorized user.',
                'headers': get_auth_header(service_accounts['user']),
                'role_id': role_id,
                'expected_resp': 403
            },
            {
                'name': '200 returned when user is authorized.',
                'headers': admin_auth_header,
                'role_id': role_id,
                'expected_resp': 200
            },
            {
                'name': 'error returned when role does not exist.',
                'headers': admin_auth_header,
                'role_id': 'ghost_role',
                'expected_resp': 404
            },
            {
                'name': f'500 returned when getting a role when name is empty',
                'role_id': '',
                'headers': admin_auth_header,
                'expected_resp': 500
            }
        ]
        tests.extend([
            {
                'name': f'200 returned when getting a role when name is {description}',
                'role_id': role_id,
                'headers': admin_auth_header,
                'expected_resp': 200
            } for name, description in self.test_postive_names
        ])
        tests.extend([
            {
                'name': f'400 returned when getting a role when name is {description}',
                'role_id': role_id,
                'headers': admin_auth_header,
                'expected_resp': 400
            } for role_id, description in self.test_negative_names if role_id is not ''
        ])
        policy = create_test_statement("test_role")
        Role.create(directory, role_id, policy)
        [Role.create(directory, role_id, policy) for role_id, _ in self.test_postive_names]
        for test in tests:
            with self.subTest(test['name']):
                url = furl('/v1/roles/{}'.format(test['role_id']))
                headers = {'Content-Type': "application/json"}
                headers.update(test['headers'])
                resp = self.app.get(url.url, headers=headers)
                self.assertEqual(test['expected_resp'], resp.status_code)
                if test['expected_resp'] == 200:
                    expected_body = {
                        'name': test['role_id'],
                        'policy': policy
                    }
                    self.assertEqual(expected_body, json.loads(resp.body))


    def test_put_role_id_policy(self):
        role_id = 'test_put_role_id_policy'
        policy_1 = create_test_statement(role_id)
        policy_2 = create_test_statement('ABCD')
        policy_invalid = "invalid policy"
        Role.create(directory, role_id, policy_1)
        admin_auth_header = get_auth_header(service_accounts['admin'])
        tests = [
            {
                'name': '401 return when no auth headers.',
                'headers': {},
                'role_id': role_id,
                'data': {
                    'policy': policy_2
                },
                'expected_resp': 401
            },
            {
                'name': '403 return when unauthorized user.',
                'headers': get_auth_header(service_accounts['user']),
                'role_id': role_id,
                'data': {
                    'policy': policy_2
                },
                'expected_resp': 403
            },
            {
                'name': '200 returned when user is authorized.',
                'headers': admin_auth_header,
                'role_id': role_id,
                'data': {
                    'policy': policy_2
                },
                'expected_resp': 200
            },
            {
                'name': '400 returned when an invalid policy is used.',
                'headers': admin_auth_header,
                'role_id': role_id,
                'data': {
                    'policy': policy_invalid
                },
                'expected_resp': 400
            },
            {
                'name': '404 returned when role does not exist.',
                'headers': admin_auth_header,
                'role_id': 'ghost_role',
                'data': {
                    'policy': policy_2
                },
                'expected_resp': 404
            }
        ]
        for test in tests:
            with self.subTest(test['name']):
                headers = {'Content-Type': "application/json"}
                headers.update(test['headers'])
                url = furl(f"/v1/roles/{test['role_id']}/policy")
                data = json.dumps(test['data'])
                resp = self.app.put(url.url, data=data, headers=headers)
                self.assertEqual(test['expected_resp'], resp.status_code)


if __name__ == '__main__':
    unittest.main()
