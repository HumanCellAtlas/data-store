#!/usr/bin/env python
# coding: utf-8
"""
Checks to make sure that our secrets checker, that runs prior to deployment, checks dev, integration, and staging...
and doesn't check custom stage names.  Incorrect secrets on any of these 3 stages should throw a ValueError.
"""
import logging
import sys
import unittest
import os

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from scripts.check_deployment_secrets import SecretsChecker
from tests import needs_terraform


logger = logging.getLogger(__name__)


class TestSecretCheck(unittest.TestCase):
    @needs_terraform
    def test_dev_secrets(self):
        """Checks if the secrets in dev are gucci."""
        s = SecretsChecker(stage='dev')
        s.run()

    @needs_terraform
    def test_integration_secrets(self):
        """Checks if the secrets in integration are prada."""
        s = SecretsChecker(stage='integration')
        s.run()

    @needs_terraform
    def test_staging_secrets(self):
        """Checks if the secrets in staging are ready to take the fashion world by storm."""
        s = SecretsChecker(stage='staging')
        s.run()

    @needs_terraform
    def test_custom_stage_secrets(self):
        """
        This should not test other stages because we have no way of knowing what
        those secrets should be, so this should always pass for other stage names.
        """
        s = SecretsChecker(stage='somenonsensenamelikeprod')
        s.run()

    @needs_terraform
    def test_invalid_secrets(self):
        """This should raise a ValueError and warn the user since it will check dev against an unqualified email."""
        s = SecretsChecker(stage='dev')
        s.email = ['nonsense']
        with self.assertRaises(ValueError):
            s.run()


if __name__ == "__main__":
    unittest.main()
