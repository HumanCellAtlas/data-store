#!/usr/bin/env python
# coding: utf-8

"""
Functional Test of the Users API
"""
import json
import os
import sys
import unittest

from furl import furl

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests.base_api_test import BaseAPITest
from tests.common import get_auth_header, service_accounts, create_test_statement
from tests.data import TEST_NAMES_POS, TEST_NAMES_NEG
from fusillade.clouddirectory import User, Group, Role


class TestUserApi(BaseAPITest, unittest.TestCase):
    def tearDown(self):
        self.clear_directory(users=[
            service_accounts['admin']['client_email'],
        ])

    def test_post_new_user(self):
        tests = [
            {
                'name': f'201 returned when creating a user',
                'json_request_body': {
                    "user_id": "test_post_user0@email.com"

                },
                'response': {
                    'code': 201
                }
            },
            {
                'name': f'201 returned when creating a user with group only',
                'json_request_body': {
                    "user_id": "test_post_user1@email.com",
                    "groups": [Group.create("group_01").name]
                },
                'response': {
                    'code': 201
                }
            },
            {
                'name': f'201 returned when creating a user with role only',
                'json_request_body': {
                    "user_id": "test_post_user2@email.com",
                    "roles": [Role.create("role_02").name]
                },
                'response': {
                    'code': 201
                }
            },
            {
                'name': f'201 returned when creating a user with policy only',
                'json_request_body': {
                    "user_id": "test_post_user3@email.com",
                    "policy": create_test_statement("policy_03")
                },
                'response': {
                    'code': 201
                }
            },
            {
                'name': f'201 returned when creating a user with group, role and policy',
                'json_request_body': {
                    "user_id": "test_post_user4@email.com",
                    "groups": [Group.create("group_04").name],
                    "roles": [Role.create("role_04").name],
                    "policy": create_test_statement("policy_04")
                },
                'response': {
                    'code': 201
                }
            },
            {
                'name': f'400 returned when creating a user without username',
                'json_request_body': {
                    "groups": [Group.create("group_05").name],
                    "roles": [Role.create("role_05").name],
                    "policy": create_test_statement("policy_05")
                },
                'response': {
                    'code': 400
                }
            },
            {
                'name': f'409 returned when creating a user that already exists',
                'json_request_body': {
                    "user_id": "test_post_user4@email.com"
                },
                'response': {
                    'code': 409
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
                headers = {'Content-Type': "application/json"}
                headers.update(get_auth_header(service_accounts['admin']))
                if test['name'] == "409 returned when creating a user that already exists":
                    self.app.post('/v1/user', headers=headers, data=json.dumps(test['json_request_body']))
                resp = self.app.post('/v1/user', headers=headers, data=json.dumps(test['json_request_body']))
                self.assertEqual(test['response']['code'], resp.status_code)
                if resp.status_code == 201:
                    resp = self.app.get(f'/v1/user/{test["json_request_body"]["user_id"]}/', headers=headers)
                    self.assertEqual(test["json_request_body"]["user_id"], json.loads(resp.body)['user_id'])

    def test_get_users(self):
        headers = {'Content-Type': "application/json"}
        headers.update(get_auth_header(service_accounts['admin']))
        for i in range(10):
            resp = self.app.post(
                '/v1/user',
                headers=headers,
                data=json.dumps({"user_id": f"test_post_user{i}@email.com"})
            )
            self.assertEqual(201, resp.status_code)
        self._test_paging(f'/v1/users', headers, 7, 'users')

    def test_get_user(self):
        User.provision_user(service_accounts['user']['client_email'])
        headers = {'Content-Type': "application/json"}
        headers.update(get_auth_header(service_accounts['user']))
        name = service_accounts['user']['client_email']
        resp = self.app.get(f'/v1/user/test_user_api@email.com', headers=headers)
        self.assertEqual(403, resp.status_code)
        resp = self.app.get(f'/v1/user/{name}/', headers=headers)
        resp.raise_for_status()
        self.assertEqual(name, json.loads(resp.body)['user_id'])

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
                url = furl(f'/v1/user/{test["name"]}')
                query_params = {
                    'user_id': test['name'],
                    'status': test['status']
                }
                url.add(query_params=query_params)
                user = User.provision_user(test['name'])
                if test['status'] == 'disabled':
                    user.enable()
                resp = self.app.put(url.url, headers=headers)
                self.assertEqual(test['response']['code'], resp.status_code)

    def test_user_status(self):
        User.provision_user(service_accounts['user']['client_email'])
        user_id = service_accounts['user']['client_email']
        user_headers = {'Content-Type': "application/json"}
        user_headers.update(get_auth_header(service_accounts['user']))
        admin_headers = {'Content-Type': "application/json"}
        admin_headers.update(get_auth_header(service_accounts['admin']))

        disable_url = furl(f"/v1/user/{user_id}", query_params={'user_id': user_id, 'status': 'disabled'})
        enable_url = furl(f"/v1/user/{user_id}", query_params={'user_id': user_id, 'status': 'enabled'})
        test_user_url = furl(f"/v1/user/{user_id}/")

        # check user can get info
        resp = self.app.get(test_user_url.url, headers=user_headers)
        self.assertEqual(200, resp.status_code)

        # disable the user
        resp = self.app.put(disable_url.url, headers=admin_headers)
        self.assertEqual(200, resp.status_code)

        # verify that user cannot access things
        resp = self.app.get(test_user_url.url, headers=user_headers)
        self.assertEqual(403, resp.status_code)

        # enable user
        resp = self.app.put(enable_url.url, headers=admin_headers)
        self.assertEqual(200, resp.status_code)

        # verify that user has access
        resp = self.app.get(test_user_url.url, headers=user_headers)
        self.assertEqual(200, resp.status_code)

    def test_put_username_groups(self):
        tests = [
            {
                'name': "test_put_user_group0@email.com",
                'action': 'add',
                'json_request_body': {
                    "groups": [Group.create("group_0").name]
                },
                'response': {
                    'code': 200
                }
            },
            {
                'name': "test_put_user_group1@email.com",
                'action': 'remove',
                'json_request_body': {
                    "groups": [Group.create("group_1").name]
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
                url = furl(f'/v1/user/{test["name"]}/groups/')
                query_params = {
                    'user_id': test['name'],
                    'action': test['action']
                }
                url.add(query_params=query_params)
                user = User.provision_user(test['name'])
                if test['action'] == 'remove':
                    user.add_groups(test['json_request_body']['groups'])
                resp = self.app.put(url.url, headers=headers, data=data)
                self.assertEqual(test['response']['code'], resp.status_code)

    def test_get_username_groups(self):
        headers = {'Content-Type': "application/json"}
        headers.update(get_auth_header(service_accounts['admin']))
        name = "test_user_group_api@email.com"
        key = 'groups'
        user = User.provision_user(name)
        resp = self.app.get(f'/v1/user/{name}/groups', headers=headers)
        self.assertEqual(1, len(json.loads(resp.body)[key]))
        groups = [Group.create(f"group_{i}").name for i in range(10)]
        user.add_groups(groups)
        self._test_paging(f'/v1/user/{name}/groups', headers, 6, key)

    def test_put_username_roles(self):
        tests = [
            {
                'name': "test_put_user_role0@email.com",
                'action': 'add',
                'json_request_body': {
                    "roles": [Role.create("role_0").name]
                },
                'response': {
                    'code': 200
                }
            },
            {
                'name': "test_put_user_role1@email.com",
                'action': 'remove',
                'json_request_body': {
                    "roles": [Role.create("role_1").name]
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
                url = furl(f'/v1/user/{test["name"]}/roles/')
                query_params = {
                    'user_id': test['name'],
                    'action': test['action']
                }
                url.add(query_params=query_params)
                user = User.provision_user(test['name'])
                if test['action'] == 'remove':
                    user.add_roles(test['json_request_body']['roles'])
                resp = self.app.put(url.url, headers=headers, data=data)
                self.assertEqual(test['response']['code'], resp.status_code)

    def test_get_username_roles(self):
        headers = {'Content-Type': "application/json"}
        headers.update(get_auth_header(service_accounts['admin']))
        name = "test_user_role_api@email.com"
        key = 'roles'
        user = User.provision_user(name)
        resp = self.app.get(f'/v1/user/{name}/roles', headers=headers)
        user_role_names = [Role(None, role).name for role in user.roles]
        self.assertEqual(0, len(json.loads(resp.body)[key]))
        roles = [Role.create(f"role_{i}").name for i in range(11)]
        user.add_roles(roles)
        self._test_paging(f'/v1/user/{name}/roles', headers, 6, key)

    def test_user_owned(self):
        headers = {'Content-Type': "application/json"}
        headers.update(get_auth_header(service_accounts['admin']))
        name = "test_user_role_api@email.com"
        key = 'roles'
        user = User.provision_user(name)
        url = furl(f"/v1/user/{name}/owns", query_params={'resource_type': 'role'}).url
        resp = self.app.get(url, headers=headers)
        user_role_names = [Role(None, role).name for role in user.roles]
        self.assertEqual(0, len(json.loads(resp.body)[key]))
        roles = [Role.create(f"role_{i}") for i in range(11)]
        user.add_roles([role.name for role in roles])
        [user.add_ownership(role) for role in roles]
        self._test_paging(url, headers, 6, key)


if __name__ == '__main__':
    unittest.main()
