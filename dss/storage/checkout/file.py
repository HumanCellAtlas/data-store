"""
This is the module for file checkouts.
"""

import typing

from .cache_flow import get_uncached_status
from dss.config import Replica
from .common import parallel_copy


def start_file_checkout(replica: Replica, blob_key, dst_bucket: typing.Optional[str] = None,
                        file_metadata: dict = None) -> str:
    """
    Starts a file checkout.

    :param blob_key: The key of the blob that contains the file.
    :param replica: The replica to execute the checkout in.
    :param dst_bucket: If provided, check out to this bucket.  If not provided, check out to the default checkout bucket
                       for the replica.
    :param file_metadata: The metadata for the file requested, used to check if caching is required.
    :return: The execution ID of the request.
    """
    uncached_required = "True"
    if dst_bucket is None:
        dst_bucket = replica.checkout_bucket
    if "dss-checkout" in dst_bucket:
        uncached_required = get_uncached_status(file_metadata)
    source_bucket = replica.bucket
    return parallel_copy(replica, source_bucket, blob_key, dst_bucket, get_dst_key(blob_key), uncached_required)


def get_dst_key(blob_key: str):
    """
    Returns the destination key where a file checkout will be saved to.
    :param blob_key: The key for the file's data in the DSS bucket.
    :return: The key for the file's data in the checkout bucket.
    """
    return f"{blob_key}"
