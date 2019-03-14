import unittest
import os, sys
from urllib.parse import quote

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests.common import random_hex_string
from fusillade.clouddirectory import ad, cleanup_directory, cleanup_schema, publish_schema, create_directory, \
    CloudDirectory


class TestCloudDirectory(unittest.TestCase):

    def test_cd(self):
        """ Testing the process of creating and destroying an AWS CloudDirectory"""
        schema_name = "authz"
        schema_version = random_hex_string()
        directory_name = "test_dir_" + random_hex_string()

        with self.subTest("schema is published when publish_schema is called."):
            schema_arn_1 = publish_schema(schema_name, schema_version)

        with self.subTest("schema is not published when publish_schema is called a second time."):
            schema_arn_2 = publish_schema(schema_name, schema_version)
            self.assertEqual(schema_arn_1, schema_arn_2)

        with self.subTest("A CloudDirectory object is returned with schema provided when create_directory is called."):
            directory_1 = create_directory(directory_name, schema_arn_1)

        with self.subTest("A CloudDirectory object is returned with schema provided when create_directory is called"
                          " a second time"):
            directory_2 = create_directory(directory_name, schema_arn_1)
            self.assertEqual(directory_1._dir_arn, directory_2._dir_arn)

        with self.subTest("A directory is deleted when cleanup_directory is called."):
            cleanup_directory(CloudDirectory.from_name(directory_name)._dir_arn)

        with self.subTest("An error is returned when deleting a nonexistant directory."):
            self.assertRaises(ad.exceptions.AccessDeniedException, cleanup_directory, directory_1._dir_arn)

        with self.subTest("schema is deleted  when cleanup_schema is called."):
            cleanup_schema(schema_arn_1)

        with self.subTest("error returned when deleting a nonexistent schema."):
            self.assertRaises(ad.exceptions.ResourceNotFoundException, cleanup_schema, schema_arn_2)

    def test_structure(self):
        """Check that cloud directory is setup for fusillade"""
        schema_name = "authz"
        schema_version = random_hex_string()
        directory_name = "test_dir_" + random_hex_string()
        schema_arn = publish_schema(schema_name, schema_version)
        self.addCleanup(cleanup_schema, schema_arn)
        directory = create_directory(directory_name, schema_arn)
        self.addCleanup(cleanup_directory, CloudDirectory.from_name(directory_name)._dir_arn)

        folders = ['Users', 'Roles', 'Groups', 'Policies']
        for folder in folders:
            with self.subTest(f"{folder} node is created when directory is created"):
                resp = directory.get_object_information(f'/{folder}')
                self.assertTrue(resp['ObjectIdentifier'])

        roles = ['/Roles/admin', '/Roles/default_user']
        for role in roles:
            with self.subTest(f"{role} roles is created when directory is created"):
                resp = directory.get_object_information(role)
                self.assertTrue(resp['ObjectIdentifier'])

        with self.subTest("Admin users created when the directory is created"):
            user = '/Users/' +  quote("test_email@domain.com")
            resp = directory.get_object_information(user)
            self.assertTrue(resp['ObjectIdentifier'])




if __name__ == '__main__':
    unittest.main()
