#from chainedawslambda import aws
#from chainedawslambda.s3copyclient import S3ParallelCopySupervisorTask
from boto3 import s3

from dss.util.aws import get_s3_chunk_size


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

test_bucket = "org-humancellatlas-dss-dev"


def boto_copy(
        source_bucket: str, source_key: str,
        destination_bucket: str, destination_key: str):
    copy_source = {
        'Bucket': source_bucket,
        'Key': source_key
    }
    s3.copy(copy_source, destination_bucket, destination_key)



# region Description
def parallel_copy(
        source_bucket: str, source_key: str,
        destination_bucket: str, destination_key: str):
    initial_state = S3ParallelCopySupervisorTask.setup_copy_task(
        source_bucket, source_key,
        destination_bucket, destination_key,
        get_s3_chunk_size,
        3600)
    aws.schedule_task(S3ParallelCopySupervisorTask, initial_state)
# endregion


def get_src_object_name(file_metadata: dict):
    return "blobs/" + ".".join((
        file_metadata[BundleFileMeta.SHA256],
        file_metadata[BundleFileMeta.SHA1],
        file_metadata[BundleFileMeta.S3_ETAG],
        file_metadata[BundleFileMeta.CRC32C],
    ))

def get_dst_bundle_prefix(bundle_id: str, bundle_version: str) -> str:
    return "checkedout/{}.{}".format(bundle_id, bundle_version)
