import json
import unittest
import os, sys

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from fusillade.clouddirectory import Role, cleanup_directory, cleanup_schema
from tests.common import new_test_directory


class TestRole(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory, cls.schema_arn = new_test_directory()

    @classmethod
    def tearDownClass(cls):
        cleanup_directory(cls.directory._dir_arn)
        cleanup_schema(cls.schema_arn)

    def tearDown(self):
        self.directory.clear()

    def test_roles(self):
        role_name = "test_role"
        role_statement = "test_policy"
        role = Role.create(self.directory, role_name, role_statement)
        with self.subTest("a role is created when role.create is called"):
            self.assertEqual(role.name, role_name)

        with self.subTest("a roles statement is retrieved when role.statement is called"):
            self.assertEqual(role.statement, role_statement)

        with self.subTest("a roles statement is changed when role.statement is assigned"):
            role.statement = "something else"
            self.assertEqual(role.statement, "something else")


if __name__ == '__main__':
    unittest.main()
