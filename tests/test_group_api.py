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
from fusillade.clouddirectory import cleanup_directory, User, Group, Role

from tests.infra.server import ChaliceTestHarness
# ChaliceTestHarness must be imported after FUSILLADE_DIR has be set

def setUpModule():
    User.provision_user(directory, service_accounts['admin']['client_email'], roles=['admin'])


@eventually(5,1, {fusillade.errors.FusilladeException})
def tearDownModule():
    cleanup_directory(directory._dir_arn)
    if old_directory_name:
        os.environ["FUSILLADE_DIR"] = old_directory_name


class TestGroupApi(unittest.TestCase):
    test_postive_names = [('helloworl12345', "alpha numerica characters"),
         ('hello@world.com', "email format") ,
        ('hello-world=_@.,ZDc', "special characters"),
        ('HellOWoRLd', "different cases"),
        ('ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789'
        'ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789', "== 128 characters"),
        ("1", "one character")]
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
        directory.clear(users=[
                service_accounts['admin']['client_email']
            ])

    def test_put_new_group(self):
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
                    "roles": [Role.create(directory, "role_02").name]
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
                    "roles": [Role.create(directory, "role_04").name],
                    "policy": create_test_statement("policy_04")
                },
                'response': {
                    'code': 201
                }
            },
            {
                'name': f'400 returned when creating a group without group_id',
                'json_request_body': {
                    "roles": [Role.create(directory, "role_05").name],
                    "policy": create_test_statement("policy_05")
                },
                'response': {
                    'code': 400
                }
            },
            {
                'name': f'500 returned when creating a group that already exists',
                'json_request_body': {
                    "group_id": "Group3"
                },
                'response': {
                    'code': 500
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
        } for name, description in self.test_postive_names
        ])
        tests.extend([{
            'name': f'400 returned when creating a role when name is {description}',
            'json_request_body': {
                "group_id": name
            },
            'response': {
                'code': 400
            }
        } for name, description in self.test_negative_names
        ])
        for test in tests:
            with self.subTest(test['name']):
                headers={'Content-Type': "application/json"}
                headers.update(get_auth_header(service_accounts['admin']))
                if test['name']=="400 returned when creating a group that already exists":
                    self.app.put('/v1/groups', headers=headers, data=json.dumps(test['json_request_body']))
                resp = self.app.put('/v1/groups/', headers=headers, data=json.dumps(test['json_request_body']))
                self.assertEqual(test['response']['code'], resp.status_code)
                if resp.status_code==201:
                    resp = self.app.get(f'/v1/groups/{test["json_request_body"]["group_id"]}/', headers=headers)
                    self.assertEqual(test["json_request_body"]["group_id"], json.loads(resp.body)['name'])

    def test_get_group(self):
        headers = {'Content-Type': "application/json"}
        headers.update(get_auth_header(service_accounts['admin']))
        name = "Groupx"
        resp = self.app.get(f'/v1/groups/{name}/', headers=headers)
        self.assertEqual(404, resp.status_code)
        Group.create(directory,name)
        resp = self.app.get(f'/v1/groups/{name}/', headers=headers)
        self.assertEqual(name, json.loads(resp.body)['name'])

    def test_put_group_roles(self):
        tests = [
            {
                'group_id': "Group1",
                'action': 'add',
                'json_request_body': {
                    "roles": [Role.create(directory, "role_0").name]
                },
                'response': {
                    'code': 200
                }
            },
            {
                'group_id': "Group2",
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
                url = furl(f'/v1/groups/{test["group_id"]}/roles/')
                query_params = {
                    'group_id': test['group_id'],
                    'action': test['action']
                }
                url.add(query_params=query_params)
                group = Group.create(directory, test['group_id'])
                if test['action'] == 'remove':
                    group.add_roles(test['json_request_body']['roles'])
                resp = self.app.put(url.url, headers=headers, data=data)
                self.assertEqual(test['response']['code'], resp.status_code)

    def test_get_group_roles(self):
        headers = {'Content-Type': "application/json"}
        headers.update(get_auth_header(service_accounts['admin']))
        name = "Group1"
        group = Group.create(directory,name)
        resp = self.app.get(f'/v1/groups/{name}/roles', headers=headers)
        group_role_names = [Role(directory, None, role).name for role in group.roles]
        self.assertEqual(0, len(json.loads(resp.body)['roles']))
        group.add_roles([Role.create(directory, "role_1").name, Role.create(directory, "role_2").name])
        resp = self.app.get(f'/v1/groups/{name}/roles', headers=headers)
        self.assertEqual(2, len(json.loads(resp.body)['roles']))

if __name__ == '__main__':
    unittest.main()
