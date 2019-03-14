import unittest
import os, sys

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from fusillade.clouddirectory import User, Group, ad, cleanup_directory, cleanup_schema
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


class TestGroup(unittest.TestCase):
    def tearDown(self):
        directory.clear()

    def test_create_group(self):
        with self.subTest("an error is returned when the group has not been created."):
            try:
                Group(directory, 'not_created')
            except ad.exceptions.ResourceNotFoundException:
                pass

        with self.subTest("The group is returned when the group has been created."):
            group = Group.create(directory, "new_group", "does things")
            self.assertEqual(group.name,  "new_group")

    def test_policy(self):
        group = Group.create(directory, "new_group", "does things")
        with self.subTest("Only one policy is attached when lookup policy is called on a group without any roles"):
            policies = group.lookup_policies()
            self.assertEqual(len(policies), 1)
            self.assertEqual(policies[0], "does things")

        with self.subTest("The group policy changes when satement is set"):
            group.statement = "new thing"
            policies = group.lookup_policies()
            self.assertEqual(policies[0], "new thing")

    def test_users(self):
        emails = ["test@test.com", "why@not.com", "hello@world.com"]
        users = [User(directory, email) for email in emails]
        with self.subTest("A user is added to the group when add_users is called"):
            group = Group.create(directory, "test", "Nothing")
            user = User(directory, "another@place.com")
            group.add_users([user])
            actual_users = [i for i in group.get_users()]
            self.assertEqual(len(actual_users), 1)

        with self.subTest("Multiple users are added to the group when multiple users are passed to add_users"):
            group = Group.create(directory, "test2", "Nothing")
            group.add_users(users)
            actual_users = [i for i in group.get_users()]
            self.assertEqual(len(actual_users), 3)

        with self.subTest("Error returned when a user is added to a group it's already apart of."):
            group = Group.create(directory, "test3", "Nothing")
            group.add_users(users)
            try:
                group.add_users(users)
            except ad.exceptions.BatchWriteException:
                pass
            actual_users = [i for i in group.get_users()]
            self.assertEqual(len(actual_users), 3)

        page_limit_old = directory._page_limit
        directory._page_limit = 2
        with self.subTest("pages of users are returned when get_users is called when the number of users exceeds the "
                          "max per page limit."):
            group = Group.create(directory, "test_2", "Nothing")
            group.add_users(users)
            actual_users = [i for i in group.get_user_names(2)]
            self.assertEqual(len(actual_users), 3)
            for email in emails:
                self.assertIn(email, actual_users)

        directory._page_limit = page_limit_old


if __name__ == '__main__':
    unittest.main()
