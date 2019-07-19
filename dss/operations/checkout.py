"""
Tools for managing checkout buckets
"""
import os
import typing
import argparse
import logging
import functools
import boto3

from dss import Replica
from dss.operations import dispatch
from dss.api.bundles import get_bundle_manifest
from dss.storage.hcablobstore import compose_blob_key
from cloud_blobstore import BlobNotFoundError
from cloud_blobstore.gs import GSBlobStore
from cloud_blobstore.s3 import S3BlobStore
from google.cloud.storage import Client


logger = logging.getLogger(__name__)

checkout = dispatch.target("checkout", help=__doc__)

@functools.lru_cache()
def get_handle(replica):
    if "aws" == replica:
        return S3BlobStore(boto3.client("s3"))
    elif "gcp" == replica:
        gcp_client = Client.from_service_account_json(
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'],
        )
        return GSBlobStore(gcp_client)
    else:
        msg = f"Unknown replica {replica}"
        logger.error(msg)
        raise Exception(msg)


def verify_delete(handler, bucket, key):
    try:
        size = handler.get_size(bucket, key)
    except BlobNotFoundError:
        logger.warning(f'Unable to locate {bucket}/{key} ')
        return
    if size:
        logger.warning(f'attempting removal of key: {key}')
        handler.delete(bucket, key)
        try:
            handler.get(bucket=bucket, key=key)
        except BlobNotFoundError:
            logger.warning(f'Success! unable to locate key {key} in bucket: {bucket} ')


@checkout.action("remove_bundle_from_checkout",
                 arguments={"--replica": dict(choices=[r.name for r in Replica], required=True),
                            "--fqid": dict(nargs="+", help="bundle fqids to remove", required=True)})
def remove_bundle_from_checkout(argv: typing.List[str], args: argparse.Namespace):
    """
    Remove a bundle from the checkout bucket
    """
    replica = Replica.aws if args.replica == 'aws' else Replica.gcp
    handler = get_handle(replica=replica.name)
    bucket = replica.checkout_bucket
    for _fqid in args.fqid:
        uuid, version = _fqid.split('.', 1)
        manifest = get_bundle_manifest(replica=replica, uuid=uuid, version=version)
        if manifest is None:
            logger.warning(f"unable to locate manifest for fqid: {'fqid'}")
            continue
        for _files in manifest['files']:
            # key = compose_blob_key(_files)
            key = f'bundles/{uuid}.{version}/{_files["name"]}'
            try:
                verify_delete(handler, bucket, key)
            except BlobNotFoundError:
                continue
