#
# Load sample data bundle from data-store/data-bundle-examples
# into a (mock or actual) S3 test bucket, as described in:
#     HCA Storage System Disk Format
#     https://docs.google.com/document/d/1jQGC0Ah2gdtzUxEeVvj0OiGM9HbmOjn8wt6LazIeUhI
# This includes creating a valid bundle manifest, and creating file and bundle
# keys according to the spec. This is then used to drive indexer testing.
# The will likely be replaced eventually by the production code to populate
# bundles in S3, but at the time of this writing that is not yet available.
#

import binascii
import datetime
import hashlib
import json
import logging
import os
import uuid
from typing import Dict, Any

import boto3
from botocore.exceptions import ClientError

from tests import utils

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


def load_sample_data_bundle() -> str:
    # Needed when using moto S3 mock, in which case the required bucket does not yet exist.
    create_s3_test_bucket()

    # Load sample-data-bundles dropseq
    data_bundle_examples_path = path_to_data_bundle_examples()
    data_bundle_path = os.path.join(data_bundle_examples_path, "smartseq2", "paired_ends")
    s3_client = boto3.client('s3')
    bundle_key = create_dropseq_bundle(data_bundle_path, s3_client)
    return bundle_key


def create_s3_test_bucket() -> None:
    conn = boto3.resource('s3')
    bucket_name = utils.get_env("DSS_S3_TEST_BUCKET")
    try:
        conn.create_bucket(Bucket=bucket_name)
    except ClientError as e:
        if e.response['Error']['Code'] != 'BucketAlreadyOwnedByYou':
            log.error("An unexpected error occured when creating test bucket: %s", bucket_name)


def create_dropseq_bundle(data_bundle_path: str, s3_client: object) -> str:
    files_info = []
    # Load files marked for indexing
    for filename in ["assay.json", "project.json", "sample.json"]:
        files_info.append(create_file_info(data_bundle_path, filename, True))
    # Load files not to be indexed, to verify they are not put into the index.
    # These files do not exist in the "data-bundle-examples" subrepository.
    # Therefore, create temporary files there for the purpose of this test, then remove them.
    tmp_nonindexed_filename1 = "tmp_test_nonindexed_file1.txt"
    tmp_nonindexed_filename2 = "tmp_test_nonindexed_file2.txt"
    try:
        create_nonindexed_test_file(data_bundle_path, tmp_nonindexed_filename1)
        create_nonindexed_test_file(data_bundle_path, tmp_nonindexed_filename2)
        for filename in [tmp_nonindexed_filename1, tmp_nonindexed_filename2]:
            files_info.append(create_file_info(data_bundle_path, filename, False))
        bundle_manifest = create_bundle_manifest(files_info)
        bundle_uuid = uuid.uuid4()
        bundle_key = "bundles/{}.{}".format(bundle_uuid, create_version())
        log.debug("bundle_key=%s", bundle_key)
        upload_bundle_files(s3_client, data_bundle_path, files_info)
        upload_bundle_manifest(s3_client, bundle_key, bundle_manifest)
    finally:
        os.remove(os.path.join(data_bundle_path, tmp_nonindexed_filename1))
        os.remove(os.path.join(data_bundle_path, tmp_nonindexed_filename2))
    return bundle_key


def create_nonindexed_test_file(path, filename):
    with open(os.path.join(path, filename), "w+") as fh:
        fh.write("Temp test mock data " + filename)


def create_bundle_manifest(files_info) -> str:
    bundle_info = {}  # type: Dict[str, Any]
    bundle_info['format'] = '0.0.1'
    bundle_info['version'] = create_version()
    bundle_info['files'] = files_info
    bundle_info['creator_uid'] = 12345
    return json.dumps(bundle_info, indent=4)


def create_version():
    timestamp = datetime.datetime.utcnow()
    return timestamp.strftime("%Y-%m-%dT%H%M%S.%fZ")


def create_file_info(bundle_path: str, filename: str, indexed: bool):
    file_info = {}  # type: Dict[str, Any]
    file_path = os.path.join(bundle_path, filename)
    file_info['name'] = filename
    file_info['uuid'] = str(uuid.uuid4())
    file_info['version'] = create_version()
    file_info['content-type'] = "metadata"  # TODO What should the content type be?
    file_info['indexed'] = indexed
    add_file_hashes(file_info, file_path)
    return file_info


def add_file_hashes(file_info, file_path) -> None:
    file_info['sha256'] = compute_file_hash(hashlib.sha256(), file_path)
    file_info['sha1'] = compute_file_hash(hashlib.sha1(), file_path)
    file_info['s3-etag'] = compute_file_hash(hashlib.md5(), file_path)
    file_info['crc32c'] = compute_file_crc32(file_path)


def compute_file_hash(hasher, filename) -> str:
    BLOCKSIZE = 65536
    with open(filename, 'rb') as afile:
        buf = afile.read(BLOCKSIZE)
        while len(buf) > 0:
            hasher.update(buf)
            buf = afile.read(BLOCKSIZE)
    return hasher.hexdigest()


def compute_file_crc32(filename):
    BLOCKSIZE = 65536
    hasher = binascii.crc32(b'')
    with open(filename, 'rb') as afile:
        buf = afile.read(BLOCKSIZE)
        while len(buf) > 0:
            hasher = binascii.crc32(buf, hasher)
            buf = afile.read(BLOCKSIZE)
    return str(hasher)


def path_to_data_bundle_examples() -> str:
    data_bundle_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data-bundle-examples"))
    if not os.path.exists(os.path.join(data_bundle_path, "dropseq")):
        raise Exception(("The example data bundles are required for testing "
                         "yet the directory " + data_bundle_path + " does not contain the expected data. "
                         "Please run: \"git submodule update --init\" to populate this directory."))
    return data_bundle_path


def upload_bundle_manifest(s3_client, bundle_key, bundle_manifest: str) -> None:
    DSS_S3_TEST_BUCKET = utils.get_env("DSS_S3_TEST_BUCKET")
    log.debug("Uploading bundle manifest to bucket %s as %s: %s",
              DSS_S3_TEST_BUCKET, bundle_key, json.dumps(json.loads(bundle_manifest), indent=4))
    s3_client.put_object(Bucket=DSS_S3_TEST_BUCKET, Key=bundle_key, Body=bundle_manifest)
    s3_client.get_object(Bucket=DSS_S3_TEST_BUCKET, Key=bundle_key)
    # Verify downloaded manifest matches the original, to ensure (mock) infrastructure is working as expected
    conn = boto3.resource('s3')
    downloaded_manifest = conn.Object(DSS_S3_TEST_BUCKET, bundle_key).get()['Body'].read().decode("utf-8")
    assert (bundle_manifest == downloaded_manifest)


def upload_bundle_files(s3_client, bundle_path, files_info) -> None:
    DSS_S3_TEST_BUCKET = utils.get_env("DSS_S3_TEST_BUCKET")
    for file_info in files_info:
        filename = file_info["name"]
        file_path = os.path.join(bundle_path, filename)
        file_key = create_file_key(file_info)
        log.debug("Uploading file %s to bucket %s as %s", file_path, DSS_S3_TEST_BUCKET, file_key)
        s3_client.upload_file(Filename=file_path, Bucket=DSS_S3_TEST_BUCKET, Key=file_key)


def create_file_key(file_info) -> str:
    return "blobs/{}.{}.{}.{}".format(file_info['sha256'], file_info['sha1'], file_info['s3-etag'], file_info['crc32c'])


if __name__ == '__main__':
    bundle_key = load_sample_data_bundle()
    log.debug("Created sample bundle with key: %s", bundle_key)
