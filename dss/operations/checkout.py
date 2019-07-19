"""
Tools for managing checkout buckets
"""
import typing
import argparse
import logging

from dss import Replica
from dss.operations import dispatch
from dss import Config
from cloud_blobstore import BlobNotFoundError


logger = logging.getLogger(__name__)

checkout = dispatch.target("checkout", help=__doc__)


def verify_delete(handler, bucket, key):
    logger.warning(f'attempting removal of key: {key}')
    handler.delete(bucket, key)
    try:
        handler.get(bucket=bucket, key=key)
    except BlobNotFoundError:
        logger.warning(f'Success! unable to locate key {key} in bucket: {bucket} ')


@checkout.action("remove_checkout",
                 arguments={"--replica": dict(choices=[r.name for r in Replica], required=True),
                            "--keys": dict(nargs="+", help="keys from checkout bucket to remove", required=True)})
def remove_checkout(argv: typing.List[str], args: argparse.Namespace):
    """
    Remove a bundle from the checkout bucket
    """
    replica = Replica[args.replica]
    handler = Config.get_blobstore_handle(replica)
    bucket = replica.checkout_bucket
    for _key in args.keys:
        if 'bundle/' in _key:
            for key in handler.list(bucket, _key):
                try:
                    verify_delete(handler, bucket, key)
                except BlobNotFoundError:
                    continue
        else:
            # should handle other keys, files/blobs
            try:
                verify_delete(handler, bucket, key)
            except BlobNotFoundError:
                continue
