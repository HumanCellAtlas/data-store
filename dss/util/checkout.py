import uuid
from logging import getLogger

import chainedawslambda
from chainedawslambda import aws
from chainedawslambda.s3copyclient import S3ParallelCopySupervisorTask
from enum import Enum, auto

from dss import chained_lambda_clients
from dss.blobstore.s3 import S3BlobStore
from dss.util.aws import get_s3_chunk_size
from dss.util.bundles import get_bundle

for client_name, client_class in chained_lambda_clients():
    chainedawslambda.aws.add_client(client_name, client_class)

log = getLogger()
blobstore = S3BlobStore()


class BundleFileMeta:
    NAME = "name"
    UUID = "uuid"
    VERSION = "version"
    CONTENT_TYPE = "content-type"
    INDEXED = "indexed"
    CRC32C = "crc32c"
    S3_ETAG = "s3_etag"
    SHA1 = "sha1"
    SHA256 = "sha256"

class ValidationEnum(Enum):
    NO_SRC_BUNDLE_FOUND = auto(),
    WRONG_DST_BUCKET = auto(),
    WRONG_PERMISSIONS_DST_BUCKET = auto(),
    PASSED = auto()


test_bucket = "org-humancellatlas-dss-dev"

def parallel_copy(
        source_bucket: str, source_key: str,
        destination_bucket: str, destination_key: str):
    log.debug(f"Copy file from bucket {source_bucket} with key {source_key} to "
              f"bucket {destination_bucket} destination file: {destination_key}")
    initial_state = S3ParallelCopySupervisorTask.setup_copy_task(
        source_bucket, source_key,
        destination_bucket, destination_key,
        get_s3_chunk_size,
        3600)
    aws.schedule_task(S3ParallelCopySupervisorTask, initial_state)

def get_src_object_name(file_metadata: dict):
    return "blobs/" + ".".join((
        file_metadata[BundleFileMeta.SHA256],
        file_metadata[BundleFileMeta.SHA1],
        file_metadata[BundleFileMeta.S3_ETAG],
        file_metadata[BundleFileMeta.CRC32C],
    ))

def get_dst_bundle_prefix(bundle_id: str, bundle_version: str) -> str:
    return "checkedout/{}.{}".format(bundle_id, bundle_version)

def get_manifest_files(bundle_id: str, version: str, replica: str):
    bundleManifest = get_bundle(bundle_id, replica, version).get('bundle')
    files = bundleManifest.get('files')
    dst_bundle_prefix = get_dst_bundle_prefix(bundle_id, version)

    for file in files:
        dst_object_name = "{}/{}".format(dst_bundle_prefix, file.get('name'))
        src_object_name = get_src_object_name(file)
        yield src_object_name, dst_object_name

def validate_file_dst(dst_bucket: str, dst_key: str, replica: str):
    valid = True
    try:
        blobstore.get_all_metadata(dst_bucket, dst_key)
    except:
        valid = False
    return valid

def validate_dst_bucket(dst_bucket: str, replica: str):
    if (not blobstore.check_bucket_exists(dst_bucket)):
        return ValidationEnum.WRONG_DST_BUCKET, None
    touchRes, cause = blobstore.touch_test_file(dst_bucket)
    if (not touchRes):
        return ValidationEnum.WRONG_PERMISSIONS_DST_BUCKET, cause
#    if (not blobstore.check_bucket_permissions(dst_bucket,['WRITE'])):
#        return ValidationEnum.WRONG_PERMISSIONS_DST_BUCKET

    return ValidationEnum.PASSED, None

def get_bucket_region(bucket: str):
    return blobstore.get_bucket_region(bucket)

def get_execution_id() -> str:
    return str(uuid.uuid1())
