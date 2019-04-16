#!/usr/bin/env python
# coding: utf-8
import os
import sys
import unittest
import shutil

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests.infra import testmode
from scripts.swagger_auth import SecureSwagger, default_auth, full_auth


@testmode.standalone
class TestSecureSwagger(unittest.TestCase):
    """Tests scripts/swagger_auth.py, that it parses and makes the new swagger file correctly."""
    def setUp(self):
        # user test configs
        self.secure_auth = full_auth
        self.default_auth = default_auth

        # faulty test configs
        self.duplicates_auth = {'/collections': ['put'], '/collections': ['put']}  # noqa
        self.nonexistentpaths_auth = {'a': ['b'], 'c': ['d']}
        self.empty_auth = {}

        self.orig_swagger_path = os.path.join(pkg_root, 'dss-api.yml')
        self.swagger_path = os.path.join(pkg_root, 'test-dss-api.yml')

        # use a copy of the swagger file for testing
        shutil.copyfile(self.orig_swagger_path, self.swagger_path)

    def tearDown(self):
        # clean up the swagger copy we made
        if os.path.exists(self.swagger_path):
            os.remove(self.swagger_path)

    def test_empty_config(self):
        """An empty config should leave all endpoints open without auth."""
        empty_config = self.set_and_return_current_config(self.empty_auth)
        assert empty_config == self.empty_auth

    def test_config_with_duplicates(self):
        """Python dicts can't have duplicate keys so json only loads one."""
        duplicates_config = self.set_and_return_current_config(self.duplicates_auth)
        assert duplicates_config == self.duplicates_auth

    def test_config_with_nonexistent_paths(self):
        """If a path doesn't exist, it is ignored."""
        nonexistentpaths_config = self.set_and_return_current_config(self.nonexistentpaths_auth)
        assert nonexistentpaths_config == self.empty_auth

    def test_auth_can_be_determined_from_swagger(self):
        """
        Assert that after modifying the swagger file to require auth on all endpoints,
        the config returned dynamically matches the one originally used.
        """
        returned_secure_config = self.set_and_return_current_config(self.secure_auth)
        assert returned_secure_config == self.secure_auth

        # do the same for the defaults
        returned_default_config = self.set_and_return_current_config(self.default_auth)
        assert returned_default_config == self.default_auth

    def test_generate_swagger_consistency(self):
        """
        Makes certain that after changing the swagger file,
        if it's changed back with the same config, it's exactly the same.
        """
        # change swagger to having all auth secure_auth
        self.set_and_return_current_config(self.secure_auth)
        with open(self.swagger_path, 'r') as f:
            secure_swagger_contents = f.readlines()

        # change swagger to the hca defaults
        self.set_and_return_current_config(self.default_auth)
        with open(self.swagger_path, 'r') as f:
            hca_default_swagger_contents = f.readlines()

        # change back to having all auth secure_auth and make sure it's the same file
        self.set_and_return_current_config(self.secure_auth)
        with open(self.swagger_path, 'r') as f:
            assert secure_swagger_contents == f.readlines()

        # change back to the hca defaults and make sure it's the same file
        self.set_and_return_current_config(self.default_auth)
        with open(self.swagger_path, 'r') as f:
            assert hca_default_swagger_contents == f.readlines()

        assert hca_default_swagger_contents != secure_swagger_contents

    def test_secure_config_contains_all_endpoints(self):
        """
        Ensures that 'auth.secure_all.json' contains all endpoints
        (and so will add auth to all endpoints if used).
        """
        endpoints_from_swagger_file = SecureSwagger().get_authconfig_from_swagger(all_endpoints=True)
        assert endpoints_from_swagger_file == self.secure_auth

    def set_and_return_current_config(self, config: dict) -> dict:
        s = SecureSwagger(infile=self.swagger_path,
                          outfile=self.swagger_path,
                          config=config)
        s.make_swagger_from_authconfig()
        return s.get_authconfig_from_swagger()


if __name__ == '__main__':
    unittest.main()
