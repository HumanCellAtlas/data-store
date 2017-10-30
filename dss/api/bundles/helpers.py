import typing

from chainedawslambda import aws
from chainedawslambda.s3copyclient import S3ParallelCopySupervisorTask
from enum import Enum, auto
from ...blobstore import BlobNotFoundError, BlobStore
from ...hcablobstore import HCABlobStore
from ...util.aws import get_s3_chunk_size, AWS_MIN_CHUNK_SIZE

ASYNC_COPY_THRESHOLD = AWS_MIN_CHUNK_SIZE
"""This is the maximum file size that we will copy synchronously."""

class CopyMode(Enum):
    NO_COPY = auto()
    COPY_INLINE = auto()
    COPY_ASYNC = auto()

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

USER_OWNED_STORE = "destination";

def copyInline(handle: BlobStore, src_bucket, src_object, dst_bucket, dst_object):
    handle.copy(src_bucket, src_object, dst_bucket, dst_object)


def get_copy_mode(handle: BlobStore, file_metadata: dict, src_bucket: str, src_object_name: str, dst_bucket: str,
                  dst_object_name: str, replica: str):
    copy_mode = CopyMode.COPY_INLINE
    source_metadata = handle.get_all_metadata(src_bucket, src_object_name)
    # does object with a given name exists at the destination? if so, we can skip the copy part.
    try:
        if verify_checksum(handle, file_metadata, dst_bucket, dst_object_name):
            copy_mode = CopyMode.NO_COPY
    except BlobNotFoundError:
        pass

    if copy_mode != CopyMode.NO_COPY and replica == "aws":
#        if source_metadata['ContentLength'] > ASYNC_COPY_THRESHOLD:
        if source_metadata['ContentLength'] > 10*1024*1024:
            copy_mode = CopyMode.COPY_ASYNC
    return copy_mode

def get_src_object_name(file_metadata: dict):
    return "blobs/" + ".".join((
        file_metadata[BundleFileMeta.SHA256],
        file_metadata[BundleFileMeta.SHA1],
        file_metadata[BundleFileMeta.S3_ETAG],
        file_metadata[BundleFileMeta.CRC32C],
    ))

# this maybe configurable in the future
def get_dst_bundle_prefix(bundleManifest: dict):
    return "checkedout/{}.{}".format(bundleManifest['uuid'], bundleManifest['version'])

def verify_checksum(handle: BlobStore, file_metadata: dict, dst_bucket, dst_object_name):
    dst_checksum = handle.get_cloud_checksum(dst_bucket, dst_object_name)
    src_checksum = get_src_cheksum(file_metadata)
    return dst_checksum.lower() == src_checksum.lower()

def get_src_cheksum(file_metadata: dict):
    return  file_metadata[BundleFileMeta.S3_ETAG]


def sanity_check_dst(handle: BlobStore, json_request_body: dict):
    if (handle.check_bucket_exists(get_destination_bucket(json_request_body))):
        # TODO: add check if dst bucket is writeable,
        return True
    else:
        return False

def get_destination_bucket(json_request_body: dict):
    return json_request_body.get(USER_OWNED_STORE)

def sanity_check_src(handle: BlobStore, bundle: str):
    return True

def parallel_copy(
        source_bucket: str, source_key: str,
        destination_bucket: str, destination_key: str):
    initial_state = S3ParallelCopySupervisorTask.setup_copy_task(
        source_bucket, source_key,
        destination_bucket, destination_key,
        lambda blob_size: (5 * 1024 * 1024),
        3600)
    aws.schedule_task(S3ParallelCopySupervisorTask, initial_state)

