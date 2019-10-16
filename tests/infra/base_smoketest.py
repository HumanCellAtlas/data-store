#!/usr/bin/env python
"""
A basic integration test of the DSS. This can also be invoked via `make smoketest`.
"""
import os
import sys
import argparse
import time
import json
import tempfile
import unittest
import subprocess
import boto3
import botocore
from datetime import datetime

import flashflood
from cloud_blobstore import BlobStore

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss import Replica
from dss.storage.checkout.bundle import get_dst_bundle_prefix
from dss.util.version import datetime_to_version_format


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


def run(command, **kwargs):
    print(GREEN(command))
    try:
        return subprocess.run(command, check=True, shell=isinstance(command, str), **kwargs)
    except subprocess.CalledProcessError as e:
        parser.exit(RED(f'{parser.prog}: Exit status {e.returncode} while running "{command}". Stopping.'))


def run_for_json(command, **kwargs):
    return json.loads(run(command, stdout=subprocess.PIPE, **kwargs).stdout.decode(sys.stdout.encoding))


class BaseSmokeTest(unittest.TestCase):
    params = [
        {
            'starting_replica': Replica.aws,
            'checkout_bucket': os.environ['DSS_S3_CHECKOUT_BUCKET'],
            'test_bucket': os.environ['DSS_S3_BUCKET_TEST']
        },
        {
            'starting_replica': Replica.gcp,
            'checkout_bucket': os.environ['DSS_GS_CHECKOUT_BUCKET'],
            'test_bucket': os.environ['DSS_GS_BUCKET_TEST']
        }
    ]
    notification_bucket = os.environ['DSS_S3_BUCKET_TEST']

    @classmethod
    def setUpClass(cls):
        cls.timestamp_started = datetime_to_version_format(datetime.utcnow())
        cls.replicas = {param['starting_replica'] for param in cls.params}
        if os.path.exists("dcp-cli"):
            run("git pull --recurse-submodules", cwd="dcp-cli")
        else:
            run("git clone --depth 1 --recurse-submodules https://github.com/HumanCellAtlas/dcp-cli")
        cls.workdir = tempfile.TemporaryDirectory(dir=os.getcwd(), prefix="smoketest-", suffix=".tmp")
        if not args.clean:
            # Disable workdir destructor
            cls.workdir._finalizer.detach()  # type: ignore

        # Create a virtualenv and install the CLI
        #
        venv = os.path.join(cls.workdir.name, "venv")
        run(f"virtualenv -p {sys.executable} {venv}")
        cls.venv_bin = os.path.join(venv, "bin", "")
        run(f"{cls.venv_bin}pip install --upgrade .", cwd="dcp-cli")

        # Configure the CLI
        #
        cli_config = {"DSSClient": {"swagger_url": f"https://{os.environ['API_DOMAIN_NAME']}/v1/swagger.json"}}
        cli_config_filename = f"{cls.workdir.name}/cli_config.json"
        with open(cli_config_filename, "w") as fh2:
            fh2.write(json.dumps(cli_config))
        os.environ["HCA_CONFIG_FILE"] = f"{cls.workdir.name}/cli_config.json"

    def upload_bundle(self, replica, bucket, src_dir):
        """ uploads bundle to DSS, returns response from CLI """
        create_res = run_for_json(f"{self.venv_bin}hca dss upload "
                                  f"--replica {replica.name} "
                                  f"--staging-bucket {bucket} "
                                  f"--src-dir {src_dir}")
        return create_res

    def get_bundle(self, replica, bundle_uuid):
        """ returns bundle manifest from DSS"""
        return run_for_json(f"{self.venv_bin}hca dss get-bundle --replica {replica.name}"
                            f" --uuid {bundle_uuid}")

    def _test_get_event(self, replica, bundle_uuid, bundle_version, event_should_exist=True):
        if event_should_exist:
            res = run_for_json(f"{self.venv_bin}hca dss get-event --replica {replica.name}"
                               f" --uuid {bundle_uuid}"
                               f" --version {bundle_version}")
            self.assertEqual(res['manifest']['version'], bundle_version)
        else:
            # TODO: enable this test when flash-flood supports immediate event deletion - BrianH
            # api = f"https://{os.environ['API_DOMAIN_NAME']}/v1"
            # params = f"version={bundle_version}&replica={replica.name}"
            # res = requests.get(f"{api}/events/{bundle_uuid}?{params}")
            # self.assertEqual(404, res.status_code)
            pass

    def _test_replay_event(self, replica, bundle_uuid, bundle_version):
        res = run_for_json(f"hca dss get-events --replica aws --per-page 10 "
                           f"--from-date {self.timestamp_started}")
        for event in flashflood.replay_with_urls(res):
            doc = json.loads(event.data)
            if doc['manifest']['version'] == bundle_version:
                break
        else:
            self.assertTrue(False)
        # TODO: Figure out how to test event is deleted after flashflood is updated - BrianH

    def _test_get_bundle(self, replica, bundle_uuid):
        """ tests that a bundle can be downloaded"""
        download_res = self.get_bundle(replica, bundle_uuid)
        self.assertEqual(bundle_uuid, download_res['bundle']['uuid'])

    def checkout_initiate(self, replica, bundle_uuid):
        """ initiates a bundle checkout, return the checkout job"""
        res = run_for_json(f"{self.venv_bin}hca dss post-bundles-checkout --uuid {bundle_uuid} "
                           f"--replica {replica.name}")
        print(f"Checkout jobId: {res['checkout_job_id']}")
        return res

    def _test_checkout(self, starting_replica, bundle_uuid, bundle_version,
                       checkout_job_id, checkout_bucket, file_count):
        """ asserts that the checkout mechanism is operational """
        for i in range(10):
            res = run_for_json(f"{self.venv_bin}hca dss get-bundles-checkout --checkout-job-id {checkout_job_id} "
                               f"--replica {starting_replica.name}")
            status = res['status']
            self.assertGreater(len(status), 0)
            if status == 'RUNNING':
                time.sleep(6)
            else:
                self.assertEqual(status, 'SUCCEEDED')
                blob_handle = self.get_blobstore(starting_replica)
                object_key = get_dst_bundle_prefix(bundle_uuid, bundle_version)
                print(f"Checking bucket {checkout_bucket} "
                      f"object key: {object_key}")
                files = list(blob_handle.list(checkout_bucket, object_key))
                self.assertEqual(len(files), file_count)
                break
        else:
            self.fail("Timed out waiting for checkout job to succeed")

    def post_search_es(self, replica, es_query):
        """ post-search using es, returns post-search response """
        return run_for_json(f'{self.venv_bin}hca dss post-search  --es-query {es_query} --replica {replica.name}')

    def put_subscription(self, replica, subscription_type, query, url):

        def subscription_put_es(replica, es_query, url):
            """ creates es subscription, return the response"""
            return run_for_json([f'{self.venv_bin}hca', 'dss', 'put-subscription',
                                 '--callback-url', url,
                                 '--method', 'PUT',
                                 '--es-query', json.dumps(es_query),
                                 '--replica', replica.name])

        def subscription_put_jmespath(replica, jmespath_query, url):
            return run_for_json([f'{self.venv_bin}hca', 'dss', 'put-subscription',
                                 '--callback-url', url,
                                 '--method', 'PUT',
                                 '--jmespath-query', f"{jmespath_query}",
                                 '--replica', replica.name])

        if subscription_type == 'jmespath':
            return subscription_put_jmespath(replica, query, url)
        else:
            return subscription_put_es(replica, query, url)

    def subscription_delete(self, replica, subscription_type, uuid):
        """ delete's subscription created, should be wrapped in self.addCleanup() """
        self.addCleanup(run, f"{self.venv_bin}hca dss delete-subscription --replica {replica.name} "
                             f"--uuid {uuid} "
                             f"--subscription-type {subscription_type}")

    def _test_subscription(self, replica, subscription_id, callback_url, subscription_type):
        get_response = self.get_subscription(replica, subscription_type, subscription_id)
        self.assertEquals(subscription_id, get_response['uuid'])
        self.assertEquals(callback_url, get_response['callback_url'])

    def get_subscriptions(self, replica, subscription_type):
        """ returns all subscriptions"""
        return run_for_json(f"{self.venv_bin}hca dss get-subscriptions --replica {replica.name}"
                            f" --subscription-type {subscription_type}  ")

    def get_subscription(self, replica, subscription_type, uuid):
        """ returns all subscriptions"""
        return run_for_json(f"{self.venv_bin}hca dss get-subscription --replica {replica.name}"
                            f" --subscription-type {subscription_type} --uuid {uuid} ")

    def get_bundle_enumerations(self, replica, per_page=500, prefix=None, search_after=None, token=None):
        """returns bundle enumeration page"""
        passed_args = {"replica": replica, "per-page": per_page, "prefix": prefix,
                       "search-after": search_after, "token": token}
        command_args = [f'--{key} {value}' for key, value in passed_args.items() if value is not None]
        command = f"{self.venv_bin}hca dss get-bundles-all {' '.join(command_args)}"
        resp = run_for_json(command)
        return resp

    def _test_get_subscriptions(self, replica, requested_subscription, subscription_type):
        get_response = self.get_subscriptions(replica, subscription_type)
        list_of_subscriptions = get_response['subscriptions']
        list_of_subscription_uuids = [x['uuid'] for x in list_of_subscriptions if x['uuid']]
        self.assertIn(requested_subscription, list_of_subscription_uuids)

    @staticmethod
    def _download_bundle(replica_name: str, bundle_uuid: str, workdir: str, venv_bin: str):
        with tempfile.TemporaryDirectory(prefix=f'{workdir}/') as tempdir:
            run(f"{venv_bin}hca dss download --replica {replica_name} --bundle-uuid {bundle_uuid}"
                f" --download-dir {tempdir}")

    def _test_replica_sync(self, current_replica, bundle_uuid):
        other_replicas = self.replicas - {current_replica}
        for replica in other_replicas:
            with self.subTest(f"{current_replica.name}: Wait for the bundle to appear in the {replica} replicas"):
                for i in range(120):
                    try:
                        run(f"http -Iv --check-status "
                            f"GET https://${{API_DOMAIN_NAME}}/v1/bundles/{bundle_uuid}?replica={replica.name}")
                    except SystemExit:
                        time.sleep(1)
                    else:
                        break
                else:
                    parser.exit(RED(f"Failed to replicate bundle from {current_replica.name} to {replica.name}"))
            with self.subTest(f"{current_replica.name}: Download bundle from {replica}"):
                for i in range(3):
                    try:
                        self._download_bundle(replica.name, bundle_uuid, self.workdir.name, self.venv_bin)
                        break
                    except:  # noqa
                        print(f'Waiting for {replica.name} bundle uuid: {bundle_uuid}')
                        time.sleep(1)
                else:
                    self._download_bundle(replica.name, bundle_uuid, self.workdir.name, self.venv_bin)

    def generate_presigned_url(self, bucket, key):
        s3 = boto3.client('s3', config=botocore.client.Config(signature_version='s3v4'))
        return s3.generate_presigned_url(ClientMethod='put_object', Params=dict(Bucket=bucket,
                                         Key=key, ContentType='application/json'))

    def get_blobstore(self, replica: Replica) -> BlobStore:
        if replica is Replica.aws:
            return replica.blobstore_class.from_environment()
        elif replica is Replica.gcp:
            return replica.blobstore_class.from_auth_credentials(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
        else:
            raise NameError("Replica not found")

    @classmethod
    def tearDownClass(cls):
        if args.clean:
            print('cleaning up: ' + cls.workdir.name)
            cls.workdir.cleanup()
        else:
            print(f"Leaving temporary working directory at {cls.workdir}.", file=sys.stderr)
