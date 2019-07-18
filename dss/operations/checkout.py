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
BUCKETS = dict(
    aws=os.environ['DSS_S3_CHECKOUT_BUCKET'],
    gcp=os.environ['DSS_GS_CHECKOUT_BUCKET'],
)


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


@checkout.action("remove_bundle_from_checkout",
                 arguments={"--replica": dict(choices=[r.name for r in Replica], required=True),
                            "--fqid": dict(nargs="+", help="bundle fqids to remove", required=True)})
def remove_bundle_from_checkout(argv: typing.List[str], args: argparse.Namespace):
    """
    Remove a bundle from the checkout bucket
    """
    handler = get_handle(replica=args.replica)
    bucket = BUCKETS[args.replica]
    for _fqid in args.fquids:
        uuid, version = _fqid.split(':')
        manifest = get_bundle_manifest(replica=args.replica, uuid=uuid, version=version)
        for _files in manifest['files']:
            key = compose_blob_key(_files)
            logger.info(f'attempting removal of file: {_files["uuid"]}:{_files["version"]}')
            handler.delete(bucket, key)

            try:
                handler.get(bucket=bucket, key=key)
            except BlobNotFoundError:
                logger.info(f'Success! unable to locate file:  {_files["uuid"]}:{_files["version"]} \
                in bucket: {bucket} ')





