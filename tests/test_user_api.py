#!/usr/bin/env python
# coding: utf-8

"""
Functional Test of the Users API
"""
import json
import unittest
from furl import furl
import os
import sys

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests.base_api_test import BaseAPITest
from tests.common import get_auth_header, service_accounts, create_test_statement
from tests.data import TEST_NAMES_POS, TEST_NAMES_NEG
from fusillade import directory
from fusillade.clouddirectory import User, Group, Role


class TestUserApi(BaseAPITest, unittest.TestCase):
    def tearDown(self):
        self.clear_directory(users=[
                service_accounts['admin']['client_email']
            ])

    def test_put_new_user(self):
        tests = [
            {
                'name': f'201 returned when creating a user',
                'json_request_body': {
                    "user_id": "test_put_user0@email.com"

                },
                'response': {
                    'code': 201
                }
            },
            {
                'name': f'201 returned when creating a user with group only',
                'json_request_body': {
                    "user_id": "test_put_user1@email.com",
                    "groups": [Group.create(directory, "group_01").name]
                },
                'response': {
                    'code': 201
                }
            },
            {
                'name': f'201 returned when creating a user with role only',
                'json_request_body': {
                    "user_id": "test_put_user2@email.com",
                    "roles": [Role.create(directory, "role_02").name]
                },
                'response': {
                    'code': 201
                }
            },
            {
                'name': f'201 returned when creating a user with policy only',
                'json_request_body': {
                    "user_id": "test_put_user3@email.com",
                    "policy": create_test_statement("policy_03")
                },
                'response': {
                    'code': 201
                }
            },
            {
                'name': f'201 returned when creating a user with group, role and policy',
                'json_request_body': {
                    "user_id": "test_put_user4@email.com",
                    "groups": [Group.create(directory, "group_04").name],
                    "roles": [Role.create(directory, "role_04").name],
                    "policy": create_test_statement("policy_04")
                },
                'response': {
                    'code': 201
                }
            },
            {
                'name': f'400 returned when creating a user without username',
                'json_request_body': {
                    "groups": [Group.create(directory, "group_05").name],
                    "roles": [Role.create(directory, "role_05").name],
                    "policy": create_test_statement("policy_05")
                },
                'response': {
                    'code': 400
                }
            },
            {
                'name': f'500 returned when creating a user that already exists',
                'json_request_body': {
                    "user_id": "test_put_user4@email.com"
                },
                'response': {
                    'code': 500
                }
            }
        ]
        tests.extend([{
            'name': f'201 returned when creating a role when name is {description}',
            'json_request_body': {
                "user_id": name
            },
            'response': {
                'code': 201
            }
        } for name, description in TEST_NAMES_POS
        ])
        tests.extend([{
            'name': f'400 returned when creating a role when name is {description}',
            'json_request_body': {
                "user_id": name
            },
            'response': {
                'code': 400
            }
        } for name, description in TEST_NAMES_NEG
        ])
        for test in tests:
            with self.subTest(test['name']):
                headers={'Content-Type': "application/json"}
                headers.update(get_auth_header(service_accounts['admin']))
                if test['name']=="500 returned when creating a user that already exists":
                    self.app.put('/v1/users', headers=headers, data=json.dumps(test['json_request_body']))
                resp = self.app.put('/v1/users', headers=headers, data=json.dumps(test['json_request_body']))
                self.assertEqual(test['response']['code'], resp.status_code)
                if resp.status_code==201:
                    resp = self.app.get(f'/v1/users/{test["json_request_body"]["user_id"]}/', headers=headers)
                    self.assertEqual(test["json_request_body"]["user_id"], json.loads(resp.body)['name'])

    def test_get_user(self):
        headers = {'Content-Type': "application/json"}
        headers.update(get_auth_header(service_accounts['user']))
        name = service_accounts['user']['client_email']
        resp = self.app.get(f'/v1/users/test_user_api@email.com/', headers=headers)
        self.assertEqual(403, resp.status_code)
        resp = self.app.get(f'/v1/users/{name}/', headers=headers)
        self.assertEqual(name, json.loads(resp.body)['name'])

    def test_put_user_id(self):
        tests = [
            {
                'name': "test_put_user0@email.com",
                'status': 'enabled',
                'response': {
                    'code': 200
                }
            },
            {
                'name': "test_put_user1@email.com",
                'status': 'disabled',
                'response': {
                    'code': 200
                }
            }
        ]
        for test in tests:
            with self.subTest(test["name"]):
                headers = {'Content-Type': "application/json"}
                headers.update(get_auth_header(service_accounts['admin']))
                url = furl(f'/v1/users/{test["name"]}')
                query_params = {
                    'user_id': test['name'],
                    'status': test['status']
                }
                url.add(query_params=query_params)
                user = User.provision_user(directory, test['name'])
                if test['status'] == 'disabled':
                    user.enable()
                resp = self.app.put(url.url, headers=headers)
                self.assertEqual(test['response']['code'], resp.status_code)

    def test_put_username_groups(self):
        tests = [
            {
                'name': "test_put_user_group0@email.com",
                'action': 'add',
                'json_request_body': {
                    "groups": [Group.create(directory, "group_0").name]
                },
                'response': {
                    'code': 200
                }
            },
            {
                'name': "test_put_user_group1@email.com",
                'action': 'remove',
                'json_request_body': {
                    "groups": [Group.create(directory, "group_1").name]
                },
                'response': {
                    'code': 200
                }
            }
        ]
        for test in tests:
            with self.subTest(test['json_request_body']):
                data = json.dumps(test['json_request_body'])
                headers = {'Content-Type': "application/json"}
                headers.update(get_auth_header(service_accounts['admin']))
                url = furl(f'/v1/users/{test["name"]}/groups/')
                query_params = {
                    'user_id': test['name'],
                    'action': test['action']
                }
                url.add(query_params=query_params)
                user = User.provision_user(directory, test['name'])
                if test['action'] == 'remove':
                    user.add_groups(test['json_request_body']['groups'])
                resp = self.app.put(url.url, headers=headers, data=data)
                self.assertEqual(test['response']['code'], resp.status_code)

    def test_get_username_groups(self):
        headers = {'Content-Type': "application/json"}
        headers.update(get_auth_header(service_accounts['admin']))
        name = "test_user_group_api@email.com"
        user = User.provision_user(directory, name)
        resp = self.app.get(f'/v1/users/{name}/groups', headers=headers)
        self.assertEqual(0, len(json.loads(resp.body)['groups']))
        user.add_groups([Group.create(directory, "group_0").name, Group.create(directory, "group_1").name])
        resp = self.app.get(f'/v1/users/{name}/groups', headers=headers)
        self.assertEqual(2, len(json.loads(resp.body)['groups']))

    def test_put_username_roles(self):
        tests = [
            {
                'name': "test_put_user_role0@email.com",
                'action': 'add',
                'json_request_body': {
                    "roles": [Role.create(directory, "role_0").name]
                },
                'response': {
                    'code': 200
                }
            },
            {
                'name': "test_put_user_role1@email.com",
                'action': 'remove',
                'json_request_body': {
                    "roles": [Role.create(directory, "role_1").name]
                },
                'response': {
                    'code': 200
                }
            }
        ]
        for test in tests:
            with self.subTest(test['json_request_body']):
                data = json.dumps(test['json_request_body'])
                headers = {'Content-Type': "application/json"}
                headers.update(get_auth_header(service_accounts['admin']))
                url = furl(f'/v1/users/{test["name"]}/roles/')
                query_params = {
                    'user_id': test['name'],
                    'action': test['action']
                }
                url.add(query_params=query_params)
                user = User.provision_user(directory, test['name'])
                if test['action'] == 'remove':
                    user.add_roles(test['json_request_body']['roles'])
                resp = self.app.put(url.url, headers=headers, data=data)
                self.assertEqual(test['response']['code'], resp.status_code)

    def test_get_username_roles(self):
        headers = {'Content-Type': "application/json"}
        headers.update(get_auth_header(service_accounts['admin']))
        name = "test_user_role_api@email.com"
        user = User.provision_user(directory, name)
        resp = self.app.get(f'/v1/users/{name}/roles', headers=headers)
        user_role_names = [Role(directory, None, role).name for role in user.roles]
        self.assertEqual(1, len(json.loads(resp.body)['roles']))
        self.assertEqual(user_role_names, ['default_user'])
        user.add_roles([Role.create(directory, "role_1").name, Role.create(directory, "role_2").name])
        resp = self.app.get(f'/v1/users/{name}/roles', headers=headers)
        self.assertEqual(3, len(json.loads(resp.body)['roles']))


if __name__ == '__main__':
    unittest.main()
