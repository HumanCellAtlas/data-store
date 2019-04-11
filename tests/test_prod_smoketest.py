#!/usr/bin/env python
"""
A prod test for the DSS, checks core API Requests to know whats available. Work with data present.
"""
import os, sys, argparse, time, uuid, json, shutil, tempfile, unittest
import subprocess

import boto3
import botocore
from cloud_blobstore import BlobStore
from itertools import product

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss import Replica
from tests.infra import testmode
from tests.test_smoketest.base_smoketest import BaseSmokeTest, run, run_for_json
from dss.storage.checkout.bundle import get_dst_bundle_prefix

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--no-clean", dest="clean", action="store_false",
                    help="Don't remove the temporary working directory on exit.")
args = argparse.Namespace(clean=True)

def GREEN(message=None):
    if message is None:
        return "\033[32m" if sys.stdout.isatty() else ""
    else:
        return GREEN() + str(message) + ENDC()

def RED(message=None):
    if message is None:
        return "\033[31m" if sys.stdout.isatty() else ""
    else:
        return RED() + str(message) + ENDC()

def ENDC():
    return "\033[0m" if sys.stdout.isatty() else ""


@testmode.integration
class ProdSmoketest(BaseSmokeTest):
    params = BaseSmokeTest.params

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.url = "https://www.example.com"
        cls.query = {"query": {"bool": {"must": [{"term": {"admin_deleted": "true"}}]}}}

    @classmethod
    def tearDownClass(cls):
        if args.clean:
            cls.workdir.cleanup()
        else:
            print(f"Leaving temporary working directory at {cls.workdir}.", file=sys.stderr)

    def prod_smokeTest(self):
        os.chdir(self.workdir.name)
        for param in self.params:
            replica = param['starting_replica']
            checkout_bucket = param['checkout_bucket']

            query_res = BaseSmokeTest.post_search_es(self, replica, "{}")
            bundle_uuid = query_res['results'][0]['bundle_fqid'].split('.')[0]
            bundle_version = query_res['results'][0]['bundle_fqid'].split('.')[1]

            checkout_id = BaseSmokeTest.checkout_initiate(self, replica, bundle_uuid)
            self.assertTrue(checkout_id)

            bundle_res = BaseSmokeTest.get_bundle(self, replica, bundle_uuid)
            bundle_file_count = len(bundle_res['bundle']['files'])

            subscription_id = BaseSmokeTest.subscription_create_es(self, replica, self.query, self.url)
            BaseSmokeTest._test_subscription_get_es(self, replica, subscription_id, self.url)

            BaseSmokeTest._test_subscription_get_es(self, replica, subscription_id, self.url)
            BaseSmokeTest.subscription_delete(self, replica, subscription_id)

            BaseSmokeTest._test_checkout(self, replica, bundle_uuid, bundle_version, checkout_id,
                                         checkout_bucket, bundle_file_count)

    def test_prod_smoketest(self):
        self.prod_smokeTest()


if __name__ == "__main__":
    if os.environ.get("DSS_DEPLOYMENT_STAGE") is not "prod":
        print("prod_smoketest is not applicable to stage: {}".format(os.environ.get("DSS_DEPLOYMENT_STAGE")))
        exit(0)
    else:
        args, sys.argv[1:] = parser.parse_known_args()
        unittest.main()
