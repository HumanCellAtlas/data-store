#!/usr/bin/env python
"""
A basic integration test of the DSS. This can also be invoked via `make smoketest`.
"""
import os, sys, argparse, time, uuid, json, shutil, tempfile, unittest
import subprocess

import boto3
import botocore
from cloud_blobstore import BlobStore

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.api.files import ASYNC_COPY_THRESHOLD
from tests.infra import testmode
from dss.storage.checkout import get_dst_bundle_prefix
from cloud_blobstore.s3 import S3BlobStore
from cloud_blobstore.gs import GSBlobStore


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


@testmode.integration
class Smoketest(unittest.TestCase):
    params = [
        {
            'starting_replica': "aws",
            'checkout_bucket': os.environ['DSS_S3_CHECKOUT_BUCKET'],
            'test_bucket': os.environ['DSS_S3_BUCKET_TEST']
        },
        {
            'starting_replica': "gcp",
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
        cli_config = {"DSSClient": {"swagger_url": os.environ["SWAGGER_URL"]}}
        cli_config_filename = f"{cls.workdir.name}/cli_config.json"
        with open(cli_config_filename, "w") as fh2:
            fh2.write(json.dumps(cli_config))
        os.environ["HCA_CONFIG_FILE"] = f"{cls.workdir.name}/cli_config.json"

        # Prepare the bundle using stock metadata and random data
        #
        cls.bundle_dir = os.path.join(cls.workdir.name, "bundle")
        shutil.copytree("data-bundle-examples/10X_v2/pbmc8k", cls.bundle_dir)
        with open(os.path.join(cls.bundle_dir, "async_copied_file"), "wb") as fh:
            fh.write(os.urandom(ASYNC_COPY_THRESHOLD + 1))

    def smoketest(self, starting_replica, checkout_bucket, test_bucket):
        # Tweak the metadata to a specific sample UUID
        sample_id = str(uuid.uuid4())
        run(f"cat {self.bundle_dir}/sample.json | jq .id=env.sample_id | sponge {self.bundle_dir}/sample.json",
            env=dict(os.environ, sample_id=sample_id))
        query = {'query': {'match': {'files.sample_json.id': sample_id}}}

        os.chdir(self.workdir.name)

        # Create a subscription for each replica using the query
        #
        s3 = boto3.client('s3', config=botocore.client.Config(signature_version='s3v4'))
        notifications_proofs = {}
        for replica in self.replicas:
            notification_key = f'notifications/{uuid.uuid4()}'
            url = s3.generate_presigned_url(ClientMethod='put_object',
                                            Params=dict(Bucket=self.notification_bucket,
                                                        Key=notification_key,
                                                        ContentType='application/json'))
            put_response = run_for_json([f'{self.venv_bin}hca', 'dss', 'put-subscription',
                                         '--callback-url', url,
                                         '--method', 'PUT',
                                         '--es-query', json.dumps(query),
                                         '--replica', replica])
            subscription_id = put_response['uuid']
            self.addCleanup(run, f"{self.venv_bin}hca dss delete-subscription --replica {replica} "
                                 f"--uuid {subscription_id}")
            self.addCleanup(s3.delete_object, Bucket=self.notification_bucket, Key=notification_key)
            notifications_proofs[replica] = (subscription_id, notification_key)
            get_response = run_for_json(f"{self.venv_bin}hca dss get-subscription "
                                        f"--replica {replica} "
                                        f"--uuid {subscription_id}")
            self.assertEquals(subscription_id, get_response['uuid'])
            self.assertEquals(url, get_response['callback_url'])
            list_response = run_for_json(f"{self.venv_bin}hca dss get-subscriptions --replica {replica}")
            self.assertIn(get_response, list_response['subscriptions'])

        # Create the bundle
        #
        res = run_for_json(f"{self.venv_bin}hca dss upload "
                           f"--replica {starting_replica} "
                           f"--staging-bucket {test_bucket} "
                           f"--src-dir {self.bundle_dir}")
        bundle_uuid = res['bundle_uuid']
        bundle_version = res['version']
        file_count = len(res['files'])

        # Download that bundle
        #
        run(f"{self.venv_bin}hca dss download --replica {starting_replica} --bundle-uuid {bundle_uuid}")

        # Initiate a bundle checkout
        #
        res = run_for_json(f"{self.venv_bin}hca dss post-bundles-checkout --uuid {bundle_uuid} "
                           f"--replica {starting_replica}")
        checkout_job_id = res['checkout_job_id']
        print(f"Checkout jobId: {checkout_job_id}")
        self.assertTrue(checkout_job_id)

        # Wait for the bundle to appear in the other replicas
        #
        other_replicas = self.replicas - {starting_replica}
        for replica in other_replicas:
            for i in range(10):
                try:
                    run(f"http -Iv --check-status "
                        f"GET https://${{API_DOMAIN_NAME}}/v1/bundles/{bundle_uuid}?replica={replica}")
                except SystemExit:
                    time.sleep(1)
                else:
                    break
            else:
                parser.exit(RED(f"Failed to replicate bundle from {starting_replica} to {replica}"))

            # Download bundle from other replica
            #
            run(f"{self.venv_bin}hca dss download --replica {replica} --bundle-uuid {bundle_uuid}")

        for replica in self.replicas:
            # Hit search route directly against each replica
            #
            search_route = "https://${API_DOMAIN_NAME}/v1/search"
            res = run_for_json(f'http --check {search_route} replica=={replica}',
                               input=json.dumps({'es_query': query}).encode())
            print(json.dumps(res, indent=4))
            self.assertEqual(len(res['results']), 1)

        # Wait for the checkout to complete and assert its success
        #
        for i in range(10):
            res = run_for_json(f"{self.venv_bin}hca dss get-bundles-checkout --checkout-job-id {checkout_job_id} "
                               f"--replica {starting_replica}")
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

        # Check the notifications
        #
        for replica, (subscription_id, notification_key) in notifications_proofs.items():
            obj = s3.get_object(Bucket=self.notification_bucket, Key=notification_key)
            notification = json.load(obj['Body'])
            self.assertEquals(subscription_id, notification['subscription_id'])
            self.assertEquals(bundle_uuid, notification['match']['bundle_uuid'])
            self.assertEquals(bundle_version, notification['match']['bundle_version'])

    def test_smoketest(self):
        for param in self.params:
            with self.subTest(param['starting_replica']):
                self.smoketest(**param)

                # Run a CLI search against the replicas
                #
                run(f"{self.venv_bin}hca dss post-search --es-query='{{}}' "
                    f"--replica {param['starting_replica']} > /dev/null")

    def get_blobstore(self, replica: str) -> BlobStore:
        if replica is 'aws':
            return S3BlobStore.from_environment()
        elif replica is 'gcp':
            return GSBlobStore.from_auth_credentials(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
        else:
            raise NameError("Replica not found")

    @classmethod
    def tearDownClass(cls):
        if args.clean:
            cls.workdir.cleanup()
        else:
            print(f"Leaving temporary working directory at {cls.workdir}.", file=sys.stderr)


if __name__ == "__main__":
    args, sys.argv[1:] = parser.parse_known_args()
    unittest.main()
