"""
This is the module for file checkouts.
"""

import typing

from dss.config import Replica
from .common import parallel_copy


def start_file_checkout(replica: Replica, blob_key, dst_bucket: typing.Optional[str] = None) -> str:
    """
    Starts a file checkout.

    :param blob_key: The key of the blob that contains the file.
    :param replica: The replica to execute the checkout in.
    :param dst_bucket: If provided, check out to this bucket.  If not provided, check out to the default checkout bucket
                       for the replica.
    :return: The execution ID of the request.
    """
    if dst_bucket is None:
        dst_bucket = replica.checkout_bucket
    source_bucket = replica.bucket
    return parallel_copy(replica, source_bucket, blob_key, dst_bucket, get_dst_key(blob_key))


def get_dst_key(blob_key: str):
    """
    Returns the destination key where a file checkout will be saved to.
    :param blob_key: The key for the file's data in the DSS bucket.
    :return: The key for the file's data in the checkout bucket.
    """
    return f"{blob_key}"
