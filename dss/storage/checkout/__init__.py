import io
from enum import Enum, auto
from random import choices
from string import hexdigits

from cloud_blobstore import BlobNotFoundError, BlobStoreUnknownError
from dss.config import Config, Replica
from dss.storage.bundles import get_bundle_manifest
from .common import CheckoutTokenKeys, parallel_copy
from .error import (
    BundleNotFoundError,
    CheckoutError,
    DestinationBucketNotFoundError,
    DestinationBucketNotWritableError,
    TokenError,
)


class ValidationEnum(Enum):
    NO_SRC_BUNDLE_FOUND = auto(),
    WRONG_DST_BUCKET = auto(),
    WRONG_PERMISSIONS_DST_BUCKET = auto(),
    WRONG_BUNDLE_KEY = auto(),
    PASSED = auto()


def validate_file_dst(replica: Replica, dst_bucket: str, dst_key: str):
    try:
        Config.get_blobstore_handle(replica).get_user_metadata(dst_bucket, dst_key)
        return True
    except (BlobNotFoundError, BlobStoreUnknownError):
        return False


def pre_exec_validate(
        replica: Replica, dss_bucket: str, dst_bucket: str, bundle_uuid: str, bundle_version: str) -> bool:
    validate_dst_bucket(replica, dst_bucket)
    validate_bundle_exists(replica, dss_bucket, bundle_uuid, bundle_version)
    return True


def validate_dst_bucket(replica: Replica, dst_bucket: str) -> bool:
    if not Config.get_blobstore_handle(replica).check_bucket_exists(dst_bucket):
        raise DestinationBucketNotFoundError(f"Bucket {dst_bucket} doesn't exist")
    return touch_test_file(replica, dst_bucket)


def validate_bundle_exists(replica: Replica, dss_bucket: str, bundle_uuid: str, bundle_version: str) -> bool:
    bundle_manifest = get_bundle_manifest(bundle_uuid, replica, bundle_version, bucket=dss_bucket)
    if bundle_manifest is None:
        raise BundleNotFoundError(f"Bundle {bundle_uuid}/{bundle_version} does not exist")
    return True


def touch_test_file(replica: Replica, dst_bucket: str) -> bool:
    """
    Write a test file into the specified bucket.
    :param dst_bucket: the bucket to be checked.
    :param replica: the replica to execute the checkout in.
    :return: True if able to write, if not raise DestinationBucketNotWritableError.
    """
    randomizer = ''.join(choices(hexdigits, k=2))
    # Spreading the touch test file across a larger range prevents hitting a modification rate limits.
    test_object = f"touch/{randomizer}.txt"
    handle = Config.get_blobstore_handle(replica)

    try:
        handle.upload_file_handle(
            dst_bucket,
            test_object,
            io.BytesIO(b""))
        return True
    except Exception as ex:
        raise DestinationBucketNotWritableError(ex)
    finally:
        try:
            Config.get_blobstore_handle(replica).delete(dst_bucket, test_object)
        except Exception:
            pass
