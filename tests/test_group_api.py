#!/usr/bin/env python
# coding: utf-8

"""
Functional Test of the Group API
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
from tests.data import TEST_NAMES_NEG, TEST_NAMES_POS
from fusillade.clouddirectory import Role, Group, User


class TestGroupApi(BaseAPITest, unittest.TestCase):
    def tearDown(self):
        self.clear_directory(users=[
            service_accounts['admin']['client_email']
        ])

    def test_post_group(self):
        tests = [
            {
                'name': f'201 returned when creating a group',
                'json_request_body': {
                    "group_id": "Group0"

                },
                'response': {
                    'code': 201
                }
            },
            {
                'name': f'201 returned when creating a group with role only',
                'json_request_body': {
                    "group_id": "Group1",
                    "roles": [Role.create("role_02").name]
                },
                'response': {
                    'code': 201
                }
            },
            {
                'name': f'201 returned when creating a group with policy only',
                'json_request_body': {
                    "group_id": "Group2",
                    "policy": create_test_statement("policy_03")
                },
                'response': {
                    'code': 201
                }
            },
            {
                'name': f'201 returned when creating a group with role and policy',
                'json_request_body': {
                    "group_id": "Group3",
                    "roles": [Role.create("role_04").name],
                    "policy": create_test_statement("policy_04")
                },
                'response': {
                    'code': 201
                }
            },
            {
                'name': f'400 returned when creating a group without group_id',
                'json_request_body': {
                    "roles": [Role.create("role_05").name],
                    "policy": create_test_statement("policy_05")
                },
                'response': {
                    'code': 400
                }
            },
            {
                'name': f'409 returned when creating a group that already exists',
                'json_request_body': {
                    "group_id": "Group3"
                },
                'response': {
                    'code': 409
                }
            }
        ]
        tests.extend([{
            'name': f'201 returned when creating a role when name is {description}',
            'json_request_body': {
                "group_id": name
            },
            'response': {
                'code': 201
            }
        } for name, description in TEST_NAMES_POS
        ])
        tests.extend([{
            'name': f'400 returned when creating a role when name is {description}',
            'json_request_body': {
                "group_id": name
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
                if test['name'] == "400 returned when creating a group that already exists":
                    self.app.post('/v1/group', headers=headers, data=json.dumps(test['json_request_body']))
                resp = self.app.post('/v1/group', headers=headers, data=json.dumps(test['json_request_body']))
                self.assertEqual(test['response']['code'], resp.status_code)
                if resp.status_code == 201:
                    resp = self.app.get(f'/v1/group/{test["json_request_body"]["group_id"]}/', headers=headers)
                    self.assertEqual(test["json_request_body"]["group_id"], json.loads(resp.body)['group_id'])

    def test_get_group(self):
        headers = {'Content-Type': "application/json"}
        headers.update(get_auth_header(service_accounts['admin']))
        name = "Groupx"
        resp = self.app.get(f'/v1/group/{name}/', headers=headers)
        self.assertEqual(404, resp.status_code)
        Group.create(name)
        resp = self.app.get(f'/v1/group/{name}/', headers=headers)
        self.assertEqual(name, json.loads(resp.body)['group_id'])
        self.assertTrue(json.loads(resp.body)['policies'])

    def test_get_groups(self):
        headers = {'Content-Type': "application/json"}
        headers.update(get_auth_header(service_accounts['admin']))
        for i in range(10):
            resp = self.app.post(
                '/v1/group',
                headers=headers,
                data=json.dumps({"group_id": f"test_put_group{i}",
                                 'policy': create_test_statement("test_group")})

            )
            self.assertEqual(201, resp.status_code)
        self._test_paging('/v1/groups', headers, 6, 'groups')

    def test_put_group_roles(self):
        tests = [
            {
                'group_id': "Group1",
                'action': 'add',
                'json_request_body': {
                    "roles": [Role.create("role_0").name]
                },
                'response': {
                    'code': 200
                }
            },
            {
                'group_id': "Group2",
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
                url = furl(f'/v1/group/{test["group_id"]}/roles/')
                query_params = {
                    'group_id': test['group_id'],
                    'action': test['action']
                }
                url.add(query_params=query_params)
                group = Group.create(test['group_id'])
                if test['action'] == 'remove':
                    group.add_roles(test['json_request_body']['roles'])
                resp = self.app.put(url.url, headers=headers, data=data)
                self.assertEqual(test['response']['code'], resp.status_code)

    def test_get_group_roles(self):
        headers = {'Content-Type': "application/json"}
        headers.update(get_auth_header(service_accounts['admin']))
        name = "Group1"
        key = 'roles'
        group = Group.create(name)
        resp = self.app.get(f'/v1/group/{name}/roles', headers=headers)
        group_role_names = [Role(None, role).name for role in group.roles]
        self.assertEqual(0, len(json.loads(resp.body)[key]))
        roles = [Role.create(f"role_{i}").name for i in range(10)]
        group.add_roles(roles)
        self._test_paging(f'/v1/group/{name}/roles', headers, 5, key)

    def test_get_group_users(self):
        headers = {'Content-Type': "application/json"}
        headers.update(get_auth_header(service_accounts['admin']))
        name = "Group1"
        key = 'users'
        group = Group.create(name)
        resp = self.app.get(f'/v1/group/{name}/users', headers=headers)
        group_user_names = [User(user).name for user in group.get_users_iter()]
        self.assertEqual(0, len(json.loads(resp.body)[key]))
        users = [User.provision_user(f"user_{i}", groups=[name]).name for i in range(10)]
        self._test_paging(f'/v1/group/{name}/users', headers, 5, key)

    def test_default_group(self):
        headers = {'Content-Type': "application/json"}
        users = ['admin', 'user']
        for user in users:
            with self.subTest(f"{user} has permission to access default_user group."):
                headers.update(get_auth_header(service_accounts[user]))
                resp = self.app.get(f'/v1/group/user_default', headers=headers)
                resp.raise_for_status()
                resp = self.app.get(f'/v1/group/user_default/roles', headers=headers)
                resp.raise_for_status()
                if user == 'admin':
                    resp = self.app.get(f'/v1/group/user_default/users', headers=headers)
                    resp.raise_for_status()


if __name__ == '__main__':
    unittest.main()
