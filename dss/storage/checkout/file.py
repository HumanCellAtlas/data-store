"""
This is the module for file checkouts.
"""

import typing

from .cache_flow import lookup_cache
from dss.config import Replica
from .common import parallel_copy


def start_file_checkout(replica: Replica, blob_key, dst_bucket: typing.Optional[str] = None, file_metadata: dict = None) -> str:
    """
    Starts a file checkout.

    :param blob_key: The key of the blob that contains the file.
    :param replica: The replica to execute the checkout in.
    :param dst_bucket: If provided, check out to this bucket.  If not provided, check out to the default checkout bucket
                       for the replica.
    :return: The execution ID of the request.
    """
    # TODO check if file is going to get into cache_flow, need to have class initialized beforehand for the file types.
    if dst_bucket is None:
        dst_bucket = replica.checkout_bucket
    # TODO: Add a 'checkout' tag to the AWS bucket and 'checkout' label to the google bucket in terraform to ID them
    if "dss-checkout" in dst_bucket:
        cache_required = lookup_cache(file_metadata)
    source_bucket = replica.bucket
    return parallel_copy(replica, source_bucket, blob_key, dst_bucket, get_dst_key(blob_key), cache_required)


def get_dst_key(blob_key: str):
    """
    Returns the destination key where a file checkout will be saved to.
    :param blob_key: The key for the file's data in the DSS bucket.
    :return: The key for the file's data in the checkout bucket.
    """
    return f"{blob_key}"
