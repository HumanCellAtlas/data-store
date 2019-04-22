import io
import typing

from cloud_blobstore.s3 import S3BlobStore
from cloud_blobstore import BlobStore
from cloud_blobstore import BlobNotFoundError
from enum import Enum, auto

from dss.util import multipart_parallel_upload


class ObjectTest(Enum):
    EXACT = auto()
    PREFIX = auto()


def test_object_exists(blobstore: BlobStore, bucket: str, match: str, test_type: ObjectTest = ObjectTest.EXACT) -> bool:
    """
    Test if an object exists in the BlobStore
    :param blobstore: the blobstore to check
    :param bucket: the bucket to check in the blobstore
    :param match: the string to match against; this is _not_ a regex pattern, strings must match exactly
    :param test_type: the type of test to conduct, prefix matches test if the object name starts with the match string,
        exact matches must match the full string
    :return: test bool
    """
    if test_type == ObjectTest.PREFIX:
        try:
            blobstore.list(bucket, prefix=match).__iter__().__next__()
        except StopIteration:
            return False
        else:
            return True
    elif test_type == ObjectTest.EXACT:
        try:
            blobstore.get_user_metadata(bucket, match)
            return True
        except BlobNotFoundError:
            return False
    else:
        raise ValueError(f"Not a valid storage object test type: " + test_type.name)


def idempotent_save(blobstore: BlobStore, bucket: str, key: str, data: bytes) -> typing.Tuple[bool, bool]:
    """
    idempotent_save attempts to save an object to the BlobStore. Its return values indicate whether the save was made
    successfully and whether the operation could be completed idempotently. If the data in the blobstore does not match
    the data parameter, the data in the blobstore is _not_ overwritten.

    :param blobstore: the blobstore to save the data to
    :param bucket: the bucket in the blobstore to save the data to
    :param key: the key of the object to save
    :param data: the data to save
    :return: a tuple of booleans (was the data saved?, was the save idempotent?)
    """
    if test_object_exists(blobstore, bucket, key):
        # fetch the file metadata, compare it to what we have.
        existing_data = blobstore.get(bucket, key)
        return False, existing_data == data
    else:
        # write manifest to persistent store
        part_size = 16 * 1024 * 1024
        if isinstance(blobstore, S3BlobStore) and len(data) > part_size:
            with io.BytesIO(data) as fh:
                multipart_parallel_upload(
                    blobstore.s3_client,
                    bucket,
                    key,
                    fh,
                    part_size=part_size,
                    parallelization_factor=20
                )
        else:
            blobstore.upload_file_handle(bucket, key, io.BytesIO(data))

        return True, True
