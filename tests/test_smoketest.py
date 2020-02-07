import os
import sys
import argparse
import shutil
import boto3
import botocore
import uuid
import json
import time
import unittest

from itertools import product
pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests.infra.base_smoketest import BaseSmokeTest, run, run_for_json
from dss.api.files import ASYNC_COPY_THRESHOLD
from tests.infra import testmode

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--no-clean", dest="clean", action="store_false",
                    help="Don't remove the temporary working directory on exit.")
args = argparse.Namespace(clean=True)


@testmode.integration
class Smoketest(BaseSmokeTest):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        # Prepare the bundle using stock metadata and random data
        #
        cls.bundle_dir = os.path.join(cls.workdir.name, "bundle")
        shutil.copytree(os.environ["DSS_HOME"] + "/tests/fixtures/datafiles/example_bundle", cls.bundle_dir)
        with open(os.path.join(cls.bundle_dir, "async_copied_file"), "wb") as fh:
            fh.write(os.urandom(ASYNC_COPY_THRESHOLD + 1))

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    def smoketest(self, starting_replica, checkout_bucket, test_bucket):
        # Tweak the metadata to a specific biomaterial_id UUID
        biomaterial_id = str(uuid.uuid4())
        run(f"cat {self.bundle_dir}/cell_suspension_0.json"
            f" | jq .biomaterial_core.biomaterial_id=env.biomaterial_id"
            f" | sponge {self.bundle_dir}/cell_suspension_0.json",
            env=dict(os.environ, biomaterial_id=biomaterial_id))
        # creating both tombstone subscriptions and a bundle subscriptions
        bundle_query = {
            'query': {'match': {'files.cell_suspension_json.biomaterial_core.biomaterial_id': biomaterial_id}}
        }
        tombstone_query = {"query": {"bool": {"must": [{"term": {"admin_deleted": "true"}}]}}}
        bundle_query_jmes = "event_type=='CREATE'"
        tombstone_query_jmes = "event_type=='TOMBSTONE'"
        queries = [(bundle_query, "elasticsearch"), (tombstone_query, "elasticsearch"),
                   (bundle_query_jmes, "jmespath"), (tombstone_query_jmes, "jmespath")]

        os.chdir(self.workdir.name)

        s3 = boto3.client('s3', config=botocore.client.Config(signature_version='s3v4'))
        notifications_proofs = {}
        # Elastic Search Section
        for replica, (query, query_type) in product(self.replicas, queries):
            with self.subTest(f"{starting_replica.name}: Create a subscription for replica {replica} using the "
                              f"query: {query}"):
                notification_key = f'notifications/{uuid.uuid4()}'
                url = self.generate_presigned_url(self.notification_bucket, notification_key)
                put_response = self.put_subscription(replica, query_type, query, url)
                print(put_response)
                subscription_id = put_response['uuid']
                self.addCleanup(s3.delete_object, Bucket=self.notification_bucket, Key=notification_key)
                notifications_proofs[replica] = (subscription_id, notification_key)
                self.subTest(self._test_subscription(replica, subscription_id, url, query_type))
                self.subTest(self._test_get_subscriptions(replica, subscription_id, query_type))
                self.subscription_delete(replica, query_type, subscription_id)

        with self.subTest(f"{starting_replica.name}: Create the bundle"):
            upload_response = self.upload_bundle(starting_replica, test_bucket, self.bundle_dir)
            bundle_uuid = upload_response['bundle_uuid']
            bundle_version = upload_response['version']
            file_count = len(upload_response['files'])

        with self.subTest(f"{starting_replica.name}: Download that bundle"):
            self._test_get_bundle(starting_replica, bundle_uuid)

        with self.subTest(f"{starting_replica.name}: Initiate a bundle checkout"):
            checkout_response = self.checkout_initiate(starting_replica, bundle_uuid)
            checkout_job_id = checkout_response['checkout_job_id']
            self.assertTrue(checkout_job_id)
            self._test_replica_sync(starting_replica, bundle_uuid)

        with self.subTest(f"{starting_replica.name}: Wait for the checkout to complete and assert its success"):
            self._test_checkout(starting_replica, bundle_uuid, bundle_version,
                                checkout_job_id, checkout_bucket, file_count)

        for replica in self.replicas:
            with self.subTest(f"{starting_replica.name}: Hit search route directly against each replica {replica}"):
                search_route = "https://${API_DOMAIN_NAME}/v1/search"
                res = run_for_json(f'http --check {search_route} replica=={replica.name}',
                                   input=json.dumps({'es_query': bundle_query}).encode())
                print(json.dumps(res, indent=4))
                self.assertEqual(len(res['results']), 1)

        with self.subTest(f"{starting_replica.name}: Enumerate on created bundle uuid"):
            key_prefix = f'{bundle_uuid[0:8]}'
            bundle_match = dict(uuid=bundle_uuid, version=bundle_version)
            resp = self.get_bundle_enumerations(starting_replica.name, prefix=key_prefix)
            self.assertIn(bundle_match, resp['bundles'])

        for replica in self.replicas:
            with self.subTest(f"{starting_replica.name}: Get event for bundle",
                              uuid=bundle_uuid,
                              version=bundle_version):
                self._test_get_event(replica, bundle_uuid, bundle_version)
            with self.subTest(f"{starting_replica.name}: Replay event for bundle",
                              uuid=bundle_uuid,
                              version=bundle_version):
                self._test_replay_event(replica, bundle_uuid, bundle_version)

        for replica in self.replicas:
            with self.subTest(f"{starting_replica.name}: Tombstone the bundle on replica {replica}"):
                run_for_json(f"{self.venv_bin}dbio dss delete-bundle --uuid {bundle_uuid} --version {bundle_version} "
                             f"--reason 'smoke test' --replica {replica.name}")

        for replica, (subscription_id, notification_key) in notifications_proofs.items():
            with self.subTest(f"{starting_replica.name}: Check the notifications. "
                              f"{replica.name}, {subscription_id}, {notification_key}"):
                for i in range(20):
                    try:
                        obj = s3.get_object(Bucket=self.notification_bucket, Key=notification_key)
                    except s3.exceptions.NoSuchKey:
                        time.sleep(6)
                        continue
                    else:
                        notification = json.load(obj['Body'])
                        self.assertEqual(subscription_id, notification['subscription_id'])
                        self.assertEqual(bundle_uuid, notification['match']['bundle_uuid'])
                        self.assertEqual(bundle_version, notification['match']['bundle_version'])
                        break
                else:
                    self.fail("Timed out waiting for notification to arrive")

        for replica in self.replicas:
            with self.subTest(f"{starting_replica.name}: Get event for bundle",
                              uuid=bundle_uuid,
                              version=bundle_version):
                self._test_get_event(replica, bundle_uuid, bundle_version, event_should_exist=False)

        for replica in self.replicas:
            # Enumerations against the replicas should be done after the test_replica_sync to ensure consistency.
            with self.subTest(f'Testing Bundle Enumeration on {replica.name}'):
                page_size = 500
                first_page = self.get_bundle_enumerations(replica.name, page_size)
                self.assertEqual(first_page['per_page'], 500)
                self.assertGreater(first_page['page_count'], 0)

    def test_smoketest(self):
        for param in self.params:
            self.smoketest(**param)

            with self.subTest(f"{param['starting_replica'].name}: Run a CLI search."):
                run(f"{self.venv_bin}dbio dss post-search --es-query='{{}}' "
                    f"--replica {param['starting_replica'].name} --no-paginate > /dev/null")


if __name__ == "__main__":
    args, sys.argv[1:] = parser.parse_known_args()
    unittest.main()
