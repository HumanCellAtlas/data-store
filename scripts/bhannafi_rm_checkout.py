#!/usr/bin/env python
import boto3
import argparse
from google.cloud.storage import Client
from cloud_blobstore.s3 import S3BlobStore
from cloud_blobstore.gs import GSBlobStore


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("bundle_uuid")
parser.add_argument("--version", default=None)
parser.add_argument("--replica", required=True, choices=["aws", "gcp"])
parser.add_argument("--stage", default="dev", choices=["dev", "integration", "staging"])
args = parser.parse_args()


if "staging" == args.stage:
    bucket = f"org-humancellatlas-dss-checkout-staging"
else:
    bucket = f"org-hca-dss-checkout-{args.stage}"


if "aws" == args.replica:
    handle = S3BlobStore(boto3.client("s3"))
else:
    handle = GSBlobStore(Client())


bundle_key = f"bundles/{args.bundle_uuid}"
if args.version is not None:
    bundle_key = f"{bundle_key}.{args.version}"


for key in handle.list(bucket, bundle_key):
    print(f"deleting {bucket}/{key}")
    handle.delete(bucket, key)
