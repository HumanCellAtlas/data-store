import json
import unittest
import os, sys

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from fusillade.clouddirectory import User, Role, ad, cleanup_directory, cleanup_schema
from tests.common import new_test_directory
directory = None
schema_arn = None


def setUpModule():
    global directory, schema_arn
    directory, schema_arn = new_test_directory()


def tearDownModule():
    global directory, schema_arn
    cleanup_directory(directory._dir_arn)
    cleanup_schema(schema_arn)


class TestUser(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open(f"{pkg_root}/policies/default_user_policy.json", 'r') as fp:
            policy_json = json.load(fp)
        cls.default_policy = json.dumps(policy_json)

    def tearDown(self):
        directory.clear()

    def test_get_attributes(self):
        name = "test_get_attributes@test.com"
        user = User(directory, name)
        self.assertEqual(user.get_attributes(['name'])['name'], name)

    def test_get_user_policy(self):
        with self.subTest("new user is created when instantiating User class with an new user name "):
            name = "test_get_user_policy@test.com"
            user = User(directory, name)
            self.assertEqual(user.lookup_policies(), [self.default_policy])
        with self.subTest("an existing users info is retrieved when instantiating User class for an existing user"):
            user = User(directory, name)
            self.assertEqual(user.lookup_policies(), [self.default_policy])

    def test_set_policy(self):
        name = "test_sete_policy@test.com"
        user = User(directory, name)
        with self.subTest("The initial user policy is default_policy, when the user is first created"):
            self.assertEqual(user.lookup_policies(), [self.default_policy])

        user.statement = "Something else"
        with self.subTest("The user policy changes when set_policy is used."):
            self.assertEqual(user.lookup_policies(), ["Something else"])

    def test_roles(self):
        name = "test_sete_policy@test.com"
        test_roles = [(f"group_{i}", f"Policy_{i}") for i in range(5)]
        roles = [Role.create(directory, *i).name for i in test_roles]
        role_names, role_statements = zip(*test_roles)
        role_names = sorted(role_names)
        role_statements = sorted(role_statements)

        user = User(directory, name)
        with self.subTest("a user has the default_user roles when created."):
            self.assertEqual(user.roles, ['default_user'])

        user.remove_roles(['default_user'])
        role_name, role_statement = test_roles[0]
        with self.subTest("A user has one role when a role is added."):
            user.add_roles([role_name])
            self.assertEqual(user.roles, [role_name])

        with self.subTest("An error is raised when adding a role a user already has."):
            with self.assertRaises(ad.exceptions.BatchWriteException) as ex:
               user.add_roles([role_name])
               self.assertTrue(ex.response['Error']['Message'].endswith(
                   "LinkNameAlreadyInUseException: link name "
                   "R->FakeRole_0->test_sete_policy%40test.com is already in use"))

        with self.subTest("An error is raised when adding a role that does not exist."):
            with self.assertRaises(ad.exceptions.BatchWriteException) as ex:
               user.add_roles(["ghost_role"])
               self.assertTrue(ex.response['Error']['Message'].endswith("/ Roles / ghost_role\\' does not exist.'"))

        with self.subTest("A user inherits a roles policies when a role is added to a user."):
            self.assertListEqual(sorted(user.lookup_policies()), sorted([user.statement, role_statement]))

        with self.subTest("A role is removed from user when remove role is called."):
            user.remove_roles([role_name])
            self.assertEqual(user.roles, [])

        with self.subTest("A user has multiple roles when multiple roles are added to user."):
            user.add_roles(role_names)
            self.assertEqual(user.roles, role_names)

        with self.subTest("A user inherits multiple role policies when the user has multiple roles."):
            self.assertListEqual(sorted(user.lookup_policies()),
                                 sorted([user.statement] + role_statements))

        with self.subTest("A user's roles are listed when a listing a users roles."):
            self.assertListEqual(sorted(user.roles), sorted(role_names))

        with self.subTest("Multiple roles are removed from a user when a multiple roles are specified for removal."):
            user.remove_roles(role_names)
            self.assertEqual(user.roles, [])

    @unittest.skip("unfinished and low priority")
    def test_remove_user(self):
        name = "test_get_user_policy@test.com"
        user = User(directory, name)
        self.assertEqual(len(directory.lookup_policy(user.reference)), 1)
        user.remove_user()
        directory.lookup_policy(user.reference)

if __name__ == '__main__':
    unittest.main()
