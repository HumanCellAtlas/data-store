import unittest
import os, sys
from unittest import mock

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests.infra.testmode import standalone
from tests.common import random_hex_string, service_accounts
from fusillade.clouddirectory import cd_client, cleanup_directory, cleanup_schema, publish_schema, create_directory, \
    CloudDirectory, CloudNode

admin_email = "test_email1@domain.com,test_email2@domain.com, test_email3@domain.com "


@standalone
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
            directory_1 = create_directory(directory_name, schema_arn_1, [service_accounts['admin']['client_email']])

        with self.subTest("A CloudDirectory object is returned with schema provided when create_directory is called"
                          " a second time"):
            directory_2 = create_directory(directory_name, schema_arn_1, [service_accounts['admin']['client_email']])
            self.assertEqual(directory_1._dir_arn, directory_2._dir_arn)

        with self.subTest("A directory is deleted when cleanup_directory is called."):
            cleanup_directory(CloudDirectory.from_name(directory_name)._dir_arn)

        with self.subTest("An error is returned when deleting a nonexistant directory."):
            self.assertRaises(cd_client.exceptions.AccessDeniedException, cleanup_directory, directory_1._dir_arn)

        with self.subTest("schema is deleted  when cleanup_schema is called."):
            cleanup_schema(schema_arn_1)

        with self.subTest("error returned when deleting a nonexistent schema."):
            self.assertRaises(cd_client.exceptions.ResourceNotFoundException, cleanup_schema, schema_arn_2)

    @mock.patch.dict(os.environ, FUS_ADMIN_EMAILS=admin_email)
    def test_structure(self):
        """Check that cloud directory is setup for fusillade"""
        schema_name = "authz"
        schema_version = random_hex_string()
        directory_name = "test_dir_" + random_hex_string()
        schema_arn = publish_schema(schema_name, schema_version)
        self.addCleanup(cleanup_schema, schema_arn)
        directory = create_directory(directory_name, schema_arn, [service_accounts['admin']['client_email']])
        self.addCleanup(cleanup_directory, CloudDirectory.from_name(directory_name)._dir_arn)

        folders = ['user', 'role', 'group', 'policy']
        for folder in folders:
            with self.subTest(f"{folder} node is created when directory is created"):
                resp = directory.get_object_information(f'/{folder}')
                self.assertTrue(resp['ObjectIdentifier'])

        roles = [f"/role/{CloudNode.hash_name(name)}" for name in ['fusillade_admin', 'default_user']]
        for role in roles:
            with self.subTest(f"{role} roles is created when directory is created"):
                resp = directory.get_object_information(role)
                self.assertTrue(resp['ObjectIdentifier'])

        for admin in [service_accounts['admin']['client_email']]:
            with self.subTest(f"Admin user {admin} created when the directory is created"):
                user = '/user/' + CloudNode.hash_name(admin)
                resp = directory.get_object_information(user)
                self.assertTrue(resp['ObjectIdentifier'])

        with self.subTest(f"Public User created when the directory is created"):
            group = '/user/' + CloudNode.hash_name('public')
            resp = directory.get_object_information(group)
            self.assertTrue(resp['ObjectIdentifier'])

        with self.subTest(f"Public Group created when the directory is created"):
            group = '/group/' + CloudNode.hash_name('user_default')
            resp = directory.get_object_information(group)
            self.assertTrue(resp['ObjectIdentifier'])

            expected_tags = [
                {'Key': 'project', "Value": os.getenv("FUS_PROJECT_TAG", '')},
                {'Key': 'owner', "Value": os.getenv("FUS_OWNER_TAG", '')},
                {'Key': 'env', "Value": os.getenv("FUS_DEPLOYMENT_STAGE")},
                {'Key': 'Name', "Value": "fusillade-directory"},
                {'Key': 'managedBy', "Value": "manual"}
            ]
            response = cd_client.list_tags_for_resource(
                ResourceArn=directory._dir_arn
            )
            for tag in expected_tags:
                with self.subTest(f"Directory has {tag} tag when created"):
                    self.assertIn(tag, response['Tags'])





if __name__ == '__main__':
    unittest.main()
