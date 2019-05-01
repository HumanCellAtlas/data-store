#! /usr/bin/env python

import os
import sys
import json
import boto3
import traceback
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed


def get_bucket(deployment):
    if "prod" == deployment:
        name = "org-hca-dss-prod"
    else:
        name = f"org-humancellatlas-dss-{deployment}"
    bucket = boto3.resource("s3").Bucket(name)
    return bucket

def check_file(bucket, key):
    data = bucket.Object(key).get()['Body'].read()
    file_metadata = json.loads(data.decode("utf-8"))
    blob_key = "blobs/{}.{}.{}.{}".format(file_metadata['sha256'],
                                          file_metadata['sha1'],
                                          file_metadata['s3-etag'],
                                          file_metadata['crc32c'])
    blob_metadata = bucket.Object(blob_key).get()
    reported_size = file_metadata['size']
    actual_size = blob_metadata['ContentLength']
    if reported_size != actual_size:
        print("size mismatch:", key, blob_key)

def check_prefix(bucket, pfx):
    for item in bucket.objects.filter(Prefix=f"files/{pfx}"):
        check_file(bucket, item.key)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--deployment", "-d", default="dev", choices=["dev", "integration", "staging", "prod"])
    args = parser.parse_args()
    bucket = get_bucket(args.deployment)

    with ThreadPoolExecutor(max_workers=32) as e:
        hexd = "0123456789abcdef"
        futures = [e.submit(check_prefix, bucket, a+b) for a in hexd for b in hexd]
        for f in as_completed(futures):
            try:
                f.result()
            except:
                traceback.print_exc()
