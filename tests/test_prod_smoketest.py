#!/usr/bin/env python
"""
A prod test for the DSS, checks core API Requests to know whats available. Works with data present.
"""
import os, sys, argparse, unittest, json

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests.infra import testmode
from tests.infra.base_smoketest import BaseSmokeTest

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--no-clean", dest="clean", action="store_false",
                    help="Don't remove the temporary working directory on exit.")
args = argparse.Namespace(clean=True)


@testmode.integration
class ProdSmoketest(BaseSmokeTest):
    params = BaseSmokeTest.params

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.url = "https://www.example.com"
        # query is used to ensure we can get a non-tombstoned bundle to download; its also used to
        # check if subscriptions can be created/retrieved.
        cls.query = {"query": {"bool": {"must_not": [{"term": {"admin_deleted": "true"}}]}}}

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    def prod_smokeTest(self, **kwargs):
        os.chdir(self.workdir.name)

        replica = kwargs['starting_replica']
        checkout_bucket = kwargs['checkout_bucket']

        query_res = self.post_search_es(replica, f"'{json.dumps(self.query)}'")
        bundle_uuid = query_res['results'][0]['bundle_fqid'].split('.')[0]
        bundle_version = query_res['results'][0]['bundle_fqid'].split('.')[1]

        checkout_res = self.checkout_initiate(replica, bundle_uuid)
        checkout_job_id = checkout_res['checkout_job_id']
        self.assertTrue(checkout_job_id)

        bundle_res = self.get_bundle(replica, bundle_uuid)
        bundle_file_count = len(bundle_res['bundle']['files'])

        subscription_res = self.put_subscription(replica, "elasticsearch", self.query, self.url)
        subscription_id = subscription_res['uuid']
        self._test_subscription(replica, subscription_id, self.url, "elasticsearch")
        self.subscription_delete(replica, "elasticsearch", subscription_id)

        self._test_checkout(replica, bundle_uuid, bundle_version, checkout_job_id,
                            checkout_bucket, bundle_file_count)

    def test_prod_smoketest(self):
        for params in self.params:
            print(params)
            self.prod_smokeTest(**params)


if __name__ == "__main__":
    if os.environ.get("DSS_DEPLOYMENT_STAGE") != "prod":
        print("prod_smoketest is not applicable to stage: {}".format(os.environ.get("DSS_DEPLOYMENT_STAGE")))
        exit(0)
    else:
        args, sys.argv[1:] = parser.parse_known_args()
        unittest.main()
