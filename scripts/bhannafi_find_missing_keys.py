#!/usr/bin/env python

import os
import boto3
import argparse
from google.cloud.storage import Client
from concurrent.futures import ThreadPoolExecutor

parser = argparse.ArgumentParser()
parser.add_argument("key_list")
parser.add_argument("--stage", choices=["dev", "integration", "staging", "prod"], default="integration")
parser.add_argument("--object-type", choices=["files", "bundles", "blobs", "collections"], default="bundles")
args = parser.parse_args()

gs_client = Client.from_service_account_json(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])

if "dev" == args.stage:
    s3_bucket = boto3.resource("s3").Bucket(os.environ['DSS_S3_BUCKET'])
    gs_bucket = gs_client.bucket(os.environ['DSS_GS_BUCKET'])
elif "integration" == args.stage:
    s3_bucket = boto3.resource("s3").Bucket(os.environ['DSS_S3_BUCKET_INTEGRATION'])
    gs_bucket = gs_client.bucket(os.environ['DSS_GS_BUCKET_INTEGRATION'])
elif "staging" == args.stage:
    s3_bucket = boto3.resource("s3").Bucket(os.environ['DSS_S3_BUCKET_STAGING'])
    gs_bucket = gs_client.bucket(os.environ['DSS_GS_BUCKET_STAGING'])
else:
    s3_bucket = boto3.resource("s3").Bucket(os.environ['DSS_S3_BUCKET_PROD'])
    gs_bucket = gs_client.bucket(os.environ['DSS_GS_BUCKET_PROD'])

def s3_has_pfx(key):
    for blob in s3_bucket.objects.filter(Prefix=key):
        return True
    return False

def gs_has_pfx(key):
    for blob in gs_bucket.list_blobs(prefix=key):
        return True
    return False

with open(args.key_list, "r") as fp:
    keys = fp.read().split()

def check(pfx):
    ret = list()
    if not s3_has_pfx(pfx):
        ret.append("s3")
    if not gs_has_pfx(pfx):
        ret.append("gs")
    print(pfx, ret)

with ThreadPoolExecutor(max_workers=20) as executor:
    for key in keys:
        executor.submit(check, f"{args.object_type}/{key}")
