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
from tests import skip_on_travis


logger = logging.getLogger(__name__)


class TestSecretCheck(unittest.TestCase):
    @skip_on_travis
    def test_secrets(self):
        """Checks that the current stage's secrets conform to expected values, else this will raise a ValueError."""
        s = SecretsChecker(stage=os.environ['DSS_DEPLOYMENT_STAGE'])
        s.run()

    @skip_on_travis
    def test_custom_stage_secrets(self):
        """
        This should not test other stages because we have no way of knowing what
        those secrets should be, so this should always pass for other stage names.
        """
        s = SecretsChecker(stage='somenonsensenamelikeprod')
        s.run()

    @skip_on_travis
    def test_invalid_secrets(self):
        """Checks that a ValueError is raised when an unqualified email is stored in a secret."""
        s = SecretsChecker(stage='dev')
        s.email = ['nonsense']
        with self.assertRaises(ValueError):
            s.run()


if __name__ == "__main__":
    unittest.main()
