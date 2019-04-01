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
from tests import test_smoketest
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


def run(command, **kwargs):
    print(GREEN(command))
    try:
        return subprocess.run(command, check=True, shell=isinstance(command, str), **kwargs)
    except subprocess.CalledProcessError as e:
        parser.exit(RED(f'{parser.prog}: Exit status {e.returncode} while running "{command}". Stopping.'))


def run_for_json(command, **kwargs):
    return json.loads(run(command, stdout=subprocess.PIPE, **kwargs).stdout.decode(sys.stdout.encoding))


# @testmode.integration
class ProdSmoketest(unittest.TestCase):
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
        cls.prod_bundle_id = None
        cls.prod_bundle_version = None
        cls.prod_bundle_file_count = None

    def _test_query_es(self, starting_replica):
        with self.subTest(f"{starting_replica.name}: Querying ES "):
            es_res = run_for_json('hca dss post-search  --es-query {} --replica aws')
            bundle_fqid = es_res['results'][0]['bundle_fqid']
            return bundle_fqid.split('.')[0], bundle_fqid.split('.')[1]

    def _test_checkout_start(self, starting_replica, bundle_uuid):
        res = run_for_json(f"{self.venv_bin}hca dss post-bundles-checkout --uuid {bundle_uuid} "
                               f"--replica {starting_replica.name}")
        checkout_job_id = res['checkout_job_id']
        print(f"Checkout jobId: {checkout_job_id}")
        self.assertTrue(checkout_job_id)
        return checkout_job_id

    def _test_get_bundle(self, starting_replica, bundle_uuid):
        download_res = run_for_json(f"{self.venv_bin}hca dss get-bundle --replica {starting_replica.name}"
                                    f" --uuid {bundle_uuid}")
        prod_bundle_file_count = len(download_res['bundle']['files'])
        self.assertEqual(bundle_uuid, download_res['bundle']['uuid'])
        return prod_bundle_file_count

    def _test_checkout(self, starting_replica, bundle_uuid, checkout_job_id, checkout_bucket, file_count):
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
                object_key = get_dst_bundle_prefix(bundle_uuid, self.prod_bundle_version)
                print(f"Checking bucket {checkout_bucket} "
                      f"object key: {object_key}")
                files = list(blob_handle.list(checkout_bucket, object_key))
                self.assertEqual(len(files), file_count)
                break
        else:
            self.fail("Timed out waiting for checkout job to succeed")

    def _test_subscription_create(self, starting_replica):
        s3 = boto3.client('s3', config=botocore.client.Config(signature_version='s3v4'))
        with self.subTest(f"{starting_replica.name}: Create a subscription for replica {starting_replica.name}"
                          f" using the query: { '{}' }"):
            url = 'https://www.example.com'
            put_response = run_for_json([f'{self.venv_bin}hca', 'dss', 'put-subscription',
                                         '--callback-url', url,
                                         '--es-query', " {}",
                                         '--replica', starting_replica.name])
            subscription_id = put_response['uuid']
            return subscription_id

    def _test_subscription_fetch(self, starting_replica, subscription_id):
        get_response = run_for_json(f"{self.venv_bin}hca dss get-subscription "
                                    f"--replica {starting_replica.name} "
                                    f"--uuid {subscription_id} "
                                    "--subscription-type elasticsearch")
        self.assertEquals(subscription_id, get_response['uuid'])

    def _test_subscription_delete(self,starting_replica, subscription_id):
        delete_res = run_for_json(f"{self.venv_bin}hca dss delete-subscription --replica {starting_replica.name} "
                                  f"--uuid {subscription_id} "
                                  "--subscription-type elasticsearch")
        self.assertIs(200, delete_res['status'])

    def test_prod_smoketest(self):
        os.chdir(self.workdir.name)
        for param in self.params:
            replica = param['starting_replica']
            checkout_bucket = param['checkout_bucket']

            bundle_uuid, bundle_version = self._test_query_es(replica)
            checkout_id = self._test_checkout_start(replica, bundle_uuid)
            bundle_file_count = self._test_get_bundle(replica, bundle_uuid)

            subscription_id = self._test_subscription_create(starting_replica=replica)
            self._test_subscription_fetch(replica, subscription_id)
            self._test_subscription_delete(replica, subscription_id)
            self._test_checkout(replica, bundle_uuid, checkout_id, checkout_bucket, bundle_file_count)

    def get_blobstore(self, replica: Replica) -> BlobStore:
        if replica is Replica.aws:
            return replica.blobstore_class.from_environment()
        elif replica is Replica.gcp:
            return replica.blobstore_class.from_auth_credentials(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
        else:
            raise NameError("Replica not found")


if __name__ == "__main__":
    if os.environ.get("DSS_DEPLOYMENT_STAGE") is not "prod":
        print("prod_smoketest is not applicable")
        #  TODO exit(0)
    #else:
        args, sys.argv[1:] = parser.parse_known_args()
        unittest.main()
