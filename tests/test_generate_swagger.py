#!/usr/bin/env python
# coding: utf-8

import os
import sys
import unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from scripts.generate_swagger import SecureSwagger


class TestSecureSwagger(unittest.TestCase):
    def setUp(self):
        self.infile = os.path.join(pkg_root, 'swagger_template')
        self.outfile = os.path.join(pkg_root, 'dss-api.yml')
        self.config = os.path.join(pkg_root, 'swagger_security_config.json')

    def test_lines_match(self):
        """
        A dummy check to make sure that two lines are
        added for every one line in the config.
        """
        pass

    def test_generate_swagger(self):
        s = SecureSwagger(self.infile, self.outfile, self.config)
        s.generate_swagger_with_secure_endpoints()
        # Just a stub for a future test atm


if __name__ == '__main__':
    unittest.main()
