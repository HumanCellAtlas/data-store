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

from tests.infra import testmode
from dss.config import DeploymentStage, Config, BucketConfig, Replica


@testmode.standalone
class TestConfig(unittest.TestCase):
    init_config = None

    def setUpModule(self):
        self.init_config = Config._CURRENT_CONFIG

    def tearDownModule(self):
        Config.set_config(self.init_config)

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

    def test_s3_checkout_bucket(self):
        Config.set_config(BucketConfig.NORMAL)
        self.assertEquals(Config.get_s3_checkout_bucket(), os.environ["DSS_S3_CHECKOUT_BUCKET"])
        Config.set_config(BucketConfig.TEST)
        self.assertEquals(Config.get_s3_checkout_bucket(), os.environ["DSS_S3_CHECKOUT_BUCKET_TEST"])
        Config.set_config(BucketConfig.TEST_FIXTURE)
        self.assertEquals(Config.get_s3_checkout_bucket(), os.environ["DSS_S3_CHECKOUT_BUCKET_TEST_FIXTURES"])

    def test_notification_email(self):
        for bucket_config in BucketConfig:
            Config.set_config(bucket_config)
            self.assertEquals(Config.get_notification_email(), os.environ["DSS_NOTIFICATION_SENDER"])

    def test_replica(self):
        # Assert that derived properties are distinct between enum instances
        for prop in Replica.storage_schema, Replica.bucket:
            prop_vals = set(prop.getter(r) for r in Replica)
            self.assertFalse(None in prop_vals)
            self.assertEqual(len(Replica), len(prop_vals))


if __name__ == '__main__':
    unittest.main()
