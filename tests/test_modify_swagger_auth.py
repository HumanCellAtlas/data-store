#!/usr/bin/env python
# coding: utf-8
import os
import sys
import json
import unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from scripts.modify_swagger_auth import SecureSwagger, determine_auth_configuration_from_swagger


class TestSecureSwagger(unittest.TestCase):
    def setUp(self):
        self.secure_auth = os.path.join(pkg_root, 'auth.secure_all.json')
        self.hca_default_auth = os.path.join(pkg_root, 'auth.hca_default_auth.json')

        # store the current swagger contents since we'll be mutating them in the tests
        with open(os.path.join(pkg_root, 'dss-api.yml'), 'r') as f:
            self.original_contents = f.readlines()

    def tearDown(self):
        # restore the original swagger contents
        with open(os.path.join(pkg_root, 'dss-api.yml'), 'w') as f:
            for line in self.original_contents:
                f.write(line)

    def test_auth_can_be_determined_from_swagger(self):
        # assert that after modifying the swagger file to require auth on all endpoints,
        # the config returned dynamically matches the one originally used
        secure_config = self.set_and_return_current_config(self.secure_auth)
        with open(self.secure_auth) as f:
            expected_secure_config = json.loads(f.read())
        assert secure_config == expected_secure_config

        # do the same for the hca defaults
        hca_config = self.set_and_return_current_config(self.hca_default_auth)
        with open(self.hca_default_auth) as f:
            expected_hca_config = json.loads(f.read())
        assert hca_config == expected_hca_config

    def test_generate_swagger_consistency(self):
        """
        Makes certain that after changing the swagger file,
        if it's changed back with the same config, it's exactly the same.
        """
        # change swagger to having all auth secure_auth
        self.set_and_return_current_config(self.secure_auth)
        with open(os.path.join(pkg_root, 'dss-api.yml'), 'r') as f:
            secure_swagger_contents = f.readlines()

        # change swagger to the hca defaults
        self.set_and_return_current_config(self.hca_default_auth)
        with open(os.path.join(pkg_root, 'dss-api.yml'), 'r') as f:
            hca_default_swagger_contents = f.readlines()

        # change back to having all auth secure_auth and make sure it's the same file
        self.set_and_return_current_config(self.secure_auth)
        with open(os.path.join(pkg_root, 'dss-api.yml'), 'r') as f:
            assert secure_swagger_contents == f.readlines()

        # change back to the hca defaults and make sure it's the same file
        self.set_and_return_current_config(self.hca_default_auth)
        with open(os.path.join(pkg_root, 'dss-api.yml'), 'r') as f:
            assert hca_default_swagger_contents == f.readlines()

        assert hca_default_swagger_contents != secure_swagger_contents

    def test_secure_config_contains_all_endpoints(self):
        """
        Ensures that 'auth.secure_all.json' contains all endpoints
        (and so will add auth to all endpoints if used).
        """
        endpoints_from_swagger_file = determine_auth_configuration_from_swagger(ignore_auth=True)
        with open(self.secure_auth) as f:
            endpoints_from_config_file = json.loads(f.read())
        assert endpoints_from_swagger_file == endpoints_from_config_file

    @staticmethod
    def set_and_return_current_config(config_file):
        s = SecureSwagger(config=config_file)
        s.generate_swagger_with_secure_endpoints()
        return determine_auth_configuration_from_swagger()


if __name__ == '__main__':
    unittest.main()
