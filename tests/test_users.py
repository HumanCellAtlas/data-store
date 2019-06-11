import os
import sys
import unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from fusillade.errors import FusilladeHTTPException
from fusillade.clouddirectory import User, Group, Role, cd_client, cleanup_directory, cleanup_schema, \
    get_json_file, default_user_policy_path, default_user_role_path, default_group_policy_path
from tests.common import new_test_directory, create_test_statement
from tests.infra.testmode import standalone


@standalone
class TestUser(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory, cls.schema_arn = new_test_directory()
        cls.default_policy = get_json_file(default_user_policy_path)
        cls.default_user_policies = sorted([get_json_file(default_user_role_path),
                                            get_json_file(default_group_policy_path)])

    @classmethod
    def tearDownClass(cls):
        cleanup_directory(cls.directory._dir_arn)
        cleanup_schema(cls.schema_arn)

    def tearDown(self):
        self.directory.clear()

    def test_user_statement(self):
        name = "user_statement@test.com"
        User.provision_user(self.directory, name, statement=self.default_policy)
        test_user = User(self.directory, name)
        self.assertEqual(test_user.get_policy(), self.default_policy)

    def test_get_attributes(self):
        name = "test_get_attributes@test.com"
        user = User.provision_user(self.directory, name)
        self.assertEqual(user.get_attributes(['name'])['name'], name)

    def test_get_user_policy(self):
        name = "test_get_user_policy@test.com"
        user = User(self.directory, name)
        with self.subTest("new user is automatically provisioned on demand with default settings when "
                          "lookup_policy is called for a new user."):
            self.assertEqual(sorted(user.lookup_policies()), self.default_user_policies)
        with self.subTest("error is returned when provision_user is called for an existing user"):
            self.assertRaises(FusilladeHTTPException, user.provision_user, self.directory, name)
        with self.subTest("an existing users info is retrieved when instantiating User class for an existing user"):
            user = User(self.directory, name)
            self.assertEqual(sorted(user.lookup_policies()), self.default_user_policies)

    def test_get_groups(self):
        name = "test_get_groups@test.com"
        test_groups = [(f"group_{i}", create_test_statement(f"GroupPolicy{i}")) for i in range(5)]
        groups = [Group.create(self.directory, *i) for i in test_groups]

        user = User.provision_user(self.directory, name)
        with self.subTest("A user is in the public group when user is first created."):
            self.assertEqual(Group(self.directory, object_ref=user.groups[0]).name, 'user_default')

        user.add_groups([])
        with self.subTest("A user is added to no groups when add_groups is called with no groups"):
            self.assertEqual(len(user.groups), 1)

        with self.subTest("An error is returned when add a user to a group that does not exist."):
            with self.assertRaises(cd_client.exceptions.BatchWriteException) as ex:
                user.add_groups(["ghost_group"])
                self.assertTrue(ex.response['Error']['Message'].endswith("/ group / ghost_group\\' does not exist.'"))
            self.assertEqual(len(user.groups), 1)

        user.add_groups([group.name for group in groups])
        with self.subTest("A user is added to multiple groups when add_groups is called with multiple groups"):
            self.assertEqual(len(user.groups), 6)

        with self.subTest("A user inherits the groups policies when joining a group"):
            policies = set(user.lookup_policies())
            expected_policies = set([i[1] for i in test_groups])
            expected_policies.update(self.default_user_policies)
            self.assertEqual(policies, expected_policies)

    def test_remove_groups(self):
        name = "test_remove_group@test.com"
        test_groups = [(f"group_{i}", create_test_statement(f"GroupPolicy{i}")) for i in range(5)]
        groups = [Group.create(self.directory, *i).name for i in test_groups]
        user = User.provision_user(self.directory, name)
        with self.subTest("A user is removed from a group when remove_group is called for a group the user belongs "
                          "to."):
            user.add_groups(groups)
            self.assertEqual(len(user.groups), 6)
            user.remove_groups(groups)
            self.assertEqual(len(user.groups), 1)
        with self.subTest("Error is raised when removing a user from a group it's not in."):
            self.assertRaises(cd_client.exceptions.BatchWriteException, user.remove_groups, groups)
            self.assertEqual(len(user.groups), 1)
        with self.subTest("An error is raised and the user is not removed from any groups when the user is in some of "
                          "the groups to remove."):
            user.add_groups(groups[:2])
            self.assertEqual(len(user.groups), 3)
            self.assertRaises(cd_client.exceptions.BatchWriteException, user.remove_groups, groups)
            self.assertEqual(len(user.groups), 3)

    def test_set_policy(self):
        name = "test_set_policy@test.com"
        user = User.provision_user(self.directory, name)
        with self.subTest("The initial user policy is None, when the user is first created"):
            self.assertFalse(user.get_policy())

        statement = create_test_statement(f"UserPolicySomethingElse")
        user.set_policy(statement)
        with self.subTest("The user policy is set when statement setter is used."):
            self.assertEqual(user.get_policy(), statement)
            self.assertIn(statement, user.lookup_policies())

        statement = create_test_statement(f"UserPolicySomethingElse2")
        user.set_policy(statement)
        with self.subTest("The user policy changes when set_policy is used."):
            self.assertEqual(user.get_policy(), statement)
            self.assertIn(statement, user.lookup_policies())

        with self.subTest("Error raised when setting policy to an invalid statement"):
            with self.assertRaises(FusilladeHTTPException):
                user.set_policy("Something else")
            self.assertEqual(user.get_policy(), statement)

    def test_status(self):
        name = "test_set_policy@test.com"
        user = User.provision_user(self.directory, name)

        with self.subTest("A user's status is enabled when provisioned."):
            self.assertEqual(user.status, 'Enabled')
        with self.subTest("A user's status is disabled when user.disable is called."):
            user.disable()
            self.assertEqual(user.status, 'Disabled')
        with self.subTest("A user's status is enabled when user.enable is called."):
            user.enable()
            self.assertEqual(user.status, 'Enabled')

    def test_roles(self):
        name = "test_sete_policy@test.com"
        test_roles = [(f"Role_{i}", create_test_statement(f"RolePolicy{i}")) for i in range(5)]
        roles = [Role.create(self.directory, *i).name for i in test_roles]
        role_names, role_statements = zip(*test_roles)
        role_names = sorted(role_names)
        role_statements = sorted(role_statements)

        user = User.provision_user(self.directory, name)
        user_role_names = [Role(self.directory, None, role).name for role in user.roles]
        with self.subTest("a user has the default_user roles when created."):
            self.assertEqual(user_role_names, [])

        role_name, role_statement = test_roles[0]
        with self.subTest("A user has one role when a role is added."):
            user.add_roles([role_name])
            user_role_names = [Role(self.directory, None, role).name for role in user.roles]
            self.assertEqual(user_role_names, [role_name])

        with self.subTest("An error is raised when adding a role a user already has."):
            with self.assertRaises(cd_client.exceptions.BatchWriteException) as ex:
               user.add_roles([role_name])
               self.assertTrue('LinkNameAlreadyInUseException' in ex.response['Error']['Message'])

        with self.subTest("An error is raised when adding a role that does not exist."):
            with self.assertRaises(cd_client.exceptions.BatchWriteException) as ex:
                user.add_roles(["ghost_role"])
                self.assertTrue(ex.response['Error']['Message'].endswith("/ role / ghost_role\\' does not exist.'"))

        user.set_policy(self.default_policy)
        with self.subTest("A user inherits a roles policies when a role is added to a user."):
            self.assertListEqual(sorted(user.lookup_policies()),
                                 sorted([user.get_policy(), role_statement, *self.default_user_policies]))

        with self.subTest("A role is removed from user when remove role is called."):
            user.remove_roles([role_name])
            self.assertEqual(user.roles, [])

        with self.subTest("A user has multiple roles when multiple roles are added to user."):
            user.add_roles(role_names)
            user_role_names = [Role(self.directory, None, role).name for role in user.roles]
            self.assertEqual(sorted(user_role_names), sorted(role_names))

        with self.subTest("A user inherits multiple role policies when the user has multiple roles."):
            self.assertListEqual(sorted(user.lookup_policies()),
                                 sorted([user.get_policy(), *self.default_user_policies, *role_statements]))

        with self.subTest("A user's roles are listed when a listing a users roles."):
            user_role_names = [Role(self.directory, None, role).name for role in user.roles]
            self.assertListEqual(sorted(user_role_names), sorted(role_names))

        with self.subTest("Multiple roles are removed from a user when a multiple roles are specified for removal."):
            user.remove_roles(role_names)
            self.assertEqual(user.roles, [])

    def test_group_and_role(self):
        """
        A user inherits policies from groups and roles when the user is apart of a group and assigned a role.
        """
        name = "test_set_policy@test.com"
        user = User.provision_user(self.directory, name)
        test_groups = [(f"group_{i}", create_test_statement(f"GroupPolicy{i}")) for i in range(5)]
        [Group.create(self.directory, *i) for i in test_groups]
        group_names, group_statements = zip(*test_groups)
        group_names = sorted(group_names)
        group_statements = sorted(group_statements)
        test_roles = [(f"role_{i}", create_test_statement(f"RolePolicy{i}")) for i in range(5)]
        [Role.create(self.directory, *i) for i in test_roles]
        role_names, role_statements = zip(*test_roles)
        role_names = sorted(role_names)
        role_statements = sorted(role_statements)

        user.add_roles(role_names)
        user.add_groups(group_names)
        user.set_policy(self.default_policy)
        user_role_names = [Role(self.directory, object_ref=role).name for role in user.roles]
        user_group_names = [Group(self.directory, object_ref=group).name for group in user.groups]

        self.assertListEqual(sorted(user_role_names), role_names)
        self.assertEqual(sorted(user_group_names), group_names + ['user_default'])
        self.assertSequenceEqual(sorted(user.lookup_policies()), sorted(
            [user.get_policy(), *self.default_user_policies] + group_statements + role_statements)
                                 )

    def test_ownership(self):
        user = User.provision_user(self.directory, 'test_user')
        group = Group.create(self.directory, "group_ownership")
        with self.subTest("A user is not an owner when new group is created"):
            self.assertFalse(user.is_owner(group))
        user.add_ownership(group)
        with self.subTest("A user is owner after assigning ownership."):
            self.assertTrue(user.is_owner(group))
        group2 = Group.create(self.directory, "group_ownership2")
        user.add_ownership(group2)
        with self.subTest("List groups owned, when list_owned is called"):
            resp = user.list_owned(Group)
        user.remove_ownership(group)
        self.assertFalse(user.is_owner(group))

    @unittest.skip("TODO: unfinished and low priority")
    def test_remove_user(self):
        name = "test_get_user_policy@test.com"
        user = User.provision_user(self.directory, name)
        self.assertEqual(len(self.directory.lookup_policy(user.reference)), 1)
        user.remove_user()
        self.directory.lookup_policy(user.reference)


if __name__ == '__main__':
    unittest.main()
