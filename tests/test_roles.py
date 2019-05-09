import unittest
import os, sys

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from fusillade.errors import FusilladeHTTPException
from fusillade.clouddirectory import Role, cleanup_directory, cleanup_schema, get_json_file, default_role_path
from tests.common import new_test_directory, create_test_statement


class TestRole(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory, cls.schema_arn = new_test_directory()
        cls.default_role_statement = get_json_file(default_role_path)

    @classmethod
    def tearDownClass(cls):
        cleanup_directory(cls.directory._dir_arn)
        cleanup_schema(cls.schema_arn)

    def tearDown(self):
        self.directory.clear()

    def test_role_default(self):
        with self.subTest("a role is set to default policy when role.create is called without a statement."):
            role_name = "test_role_default"
            role = Role.create(self.directory, role_name)
            self.assertEqual(role.name, role_name)
            self.assertEqual(role.statement, self.default_role_statement)

    def test_role_statement(self):
        role_name = "test_role_specified"
        statement = create_test_statement(role_name)
        role = Role.create(self.directory, role_name, statement)
        with self.subTest("a role is created with specified statement when role.create is called with a statement"):
            self.assertEqual(role.name, role_name)

        with self.subTest("a roles statement is retrieved when role.statement is called"):
            self.assertEqual(role.statement, statement)

        with self.subTest("a roles statement is changed when role.statement is assigned"):
            statement = create_test_statement(f"UserPolicySomethingElse")
            role.statement = statement
            self.assertEqual(role.statement, statement)

        with self.subTest("Error raised when setting policy to an invalid statement"):
            with self.assertRaises(FusilladeHTTPException):
                role.statement = "Something else"
            self.assertEqual(role.statement, statement)

if __name__ == '__main__':
    unittest.main()
