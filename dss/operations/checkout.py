"""
Tools for managing checkout buckets
"""
import typing
import argparse
import logging
import os

from dss import Replica
from dss.operations import dispatch
from dss import Config
from cloud_blobstore import BlobNotFoundError
from dss.api.bundles import get_bundle_manifest
from dss.storage.hcablobstore import compose_blob_key

from dss.storage.identifiers import BUNDLE_PREFIX

logger = logging.getLogger(__name__)

checkout = dispatch.target("checkout", help=__doc__)


def verify_delete(handler, bucket, key):
    logger.warning(f'attempting removal of key: {bucket}/{key}')
    try:
        handler.delete(bucket=bucket, key=key)
        handler.get(bucket=bucket, key=key)
    except BlobNotFoundError:
        logger.warning(f'Success! unable to locate key {bucket}/{key} ')
    else:
        logger.warning(f'Unable to delete key: {bucket}/{key}')
        exit(1)


@checkout.action("remove_checkout",
                 arguments={"--replica": dict(choices=[r.name for r in Replica], required=True),
                            "--keys": dict(nargs="+", help="keys from checkout bucket to remove", required=False),
                            "--bundle-fqid": dict(nargs="+", help="fqid checkout bucket to remove", required=False)})
def remove_checkout(argv: typing.List[str], args: argparse.Namespace):
    """
    Remove a bundle from the checkout bucket
    """
    replica = Replica[args.replica]
    handler = Config.get_blobstore_handle(replica)
    bucket = replica.checkout_bucket
    stage = os.getenv('DSS_DEPLOYMENT_STAGE')
    logger.warning(f'Remove Checkout Started for stage: {stage}, bucket: {bucket}')
    if args.keys:
        for _key in args.keys:
            if BUNDLE_PREFIX in _key:
                # get-bundle presignedurl uses key based prefix
                for key in handler.list(bucket, _key):
                    verify_delete(handler, bucket, key)
            else:
                # should handle other keys, files/blobs
                    verify_delete(handler, bucket, key)
    elif args.bundle_fqid:
        # get-bundle that has a manifest context does is under /blob prefix
        for _fqid in args.bundle_fqid:
            uuid, version = _fqid.split('.', 1)
            manifest = get_bundle_manifest(replica=replica, uuid=uuid, version=version)
            if manifest is None:
                logger.warning(f"unable to locate manifest for fqid: {_fqid}")
                continue
            for _files in manifest['files']:
                key = compose_blob_key(_files)
                logger.info(f'attempting removal of key: {bucket}/{key}')
                verify_delete(handler, bucket, key)
