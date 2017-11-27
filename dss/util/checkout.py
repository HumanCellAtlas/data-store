import io
import uuid
from enum import Enum, auto
from logging import getLogger

import chainedawslambda
from chainedawslambda import aws
from chainedawslambda.s3copyclient import S3ParallelCopySupervisorTask

from cloud_blobstore import BlobNotFoundError, BlobStoreUnknownError
from cloud_blobstore.s3 import S3BlobStore
from dss import chained_lambda_clients, DSSException, Config
from dss.util.aws import get_s3_chunk_size
from dss.util.bundles import get_bundle, get_bundle_from_bucket

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
    WRONG_BUNDLE_KEY = auto(),
    PASSED = auto()

def parallel_copy(source_bucket: str, source_key: str, destination_bucket: str, destination_key: str):
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
    try:
        blobstore.get_all_metadata(dst_bucket, dst_key)
        return True
    except (BlobNotFoundError, BlobStoreUnknownError):
        return False

def pre_exec_validate(dss_bucket: str, dst_bucket: str, replica: str, bundle_id: str, version: str):
    cause = None
    validation_code = validate_dst_bucket(dst_bucket, replica)
    if validation_code == ValidationEnum.PASSED:
        validation_code, cause = validate_bundle_exists(replica, dss_bucket, bundle_id, version)
    return validation_code, cause

def validate_dst_bucket(dst_bucket: str, replica: str) -> ValidationEnum:
    if (not blobstore.check_bucket_exists(dst_bucket)):
        return ValidationEnum.WRONG_DST_BUCKET
    if (not touch_test_file(dst_bucket, replica)):
        return ValidationEnum.WRONG_PERMISSIONS_DST_BUCKET

    return ValidationEnum.PASSED

def validate_bundle_exists(replica: str, bucket: str, bundle_id: str, version: str):
    try:
        get_bundle_from_bucket(bundle_id, replica, version, bucket)
        return ValidationEnum.PASSED, None
    except DSSException:
        return ValidationEnum.WRONG_BUNDLE_KEY, "Bundle with specified key does not exist"

def get_bucket_region(bucket: str):
    return blobstore.get_bucket_region(bucket)

def get_execution_id() -> str:
    return str(uuid.uuid4())

def touch_test_file(dst_bucket, replica) -> bool:
    """
    Write a test file into the specified bucket.
    :param bucket: the bucket to be checked.
    :return: True if able to write, if not also returns error message as a cause
    """
    test_object = "touch.txt"
    handle, *_ = Config.get_cloud_specific_handles(replica)

    try:
        handle.upload_file_handle(
            dst_bucket,
            test_object,
            io.BytesIO(b""))
        blobstore.delete(dst_bucket, test_object)
        return True
    except Exception as e:
        return False
