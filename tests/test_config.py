#!/usr/bin/env python
# coding: utf-8

"""
Tests for dss.Config
"""

import os
import sys
import unittest
from contextlib import contextmanager

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.config import DeploymentStage


class TestConfig(unittest.TestCase):
    def test_predicates(self):
        @contextmanager
        def override_deployment_stage(stage: DeploymentStage):
            original_stage = os.environ["DSS_DEPLOYMENT_STAGE"]
            os.environ["DSS_DEPLOYMENT_STAGE"] = stage.value

            try:
                yield
            finally:
                os.environ["DSS_DEPLOYMENT_STAGE"] = original_stage

        with override_deployment_stage(DeploymentStage.DEV):
            self.assertTrue(DeploymentStage.IS_DEV())
            self.assertFalse(DeploymentStage.IS_STAGING())
            self.assertFalse(DeploymentStage.IS_PROD())

        with override_deployment_stage(DeploymentStage.STAGING):
            self.assertFalse(DeploymentStage.IS_DEV())
            self.assertTrue(DeploymentStage.IS_STAGING())
            self.assertFalse(DeploymentStage.IS_PROD())

        with override_deployment_stage(DeploymentStage.PROD):
            self.assertFalse(DeploymentStage.IS_DEV())
            self.assertFalse(DeploymentStage.IS_STAGING())
            self.assertTrue(DeploymentStage.IS_PROD())

if __name__ == '__main__':
    unittest.main()
