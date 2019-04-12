#! /usr/bin/env python

import io
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from cloud_blobstore.s3 import S3BlobStore

s3 = boto3.client("s3")
bucket = "org-humancellatlas-dss-dev"
uuid = "d82cedf1-04ab-4f86-9565-f341803f4952"
version = "2019-04-03T165319.358614Z"
key = f"bundles/{uuid}.{version}"

def get_part_layout():
    resp = s3.get_object(
        Bucket=bucket,
        Key=key
    )
    etag = resp['ETag'].replace('"', "")
    number_of_parts = int(etag.split("-")[-1])
    return number_of_parts

def download_part(part_number):
    resp = s3.get_object(
        Bucket=bucket,
        Key=key,
        PartNumber=part_number
    )
    return resp['Body'].read()

def get():
    resp = s3.get_object(
        Bucket=bucket,
        Key=key
    )
    return resp['Body'].read()

def get_parallel(factor=6):
    with ThreadPoolExecutor(max_workers=factor) as e:
        number_of_parts = get_part_layout()
        futures = {e.submit(download_part, part_number): part_number
                   for part_number in range(1, 1 + number_of_parts)}
        parts = [(future.result(), futures[future])
                 for future in as_completed(futures)]
        parts = sorted(parts, key=lambda p: p[1])
        with io.BytesIO() as fh:
            for p in parts:
                fh.write(p[0])
            manifest = json.loads(fh.getvalue())
        return manifest

start_time = time.time()
get()
print("duration:", time.time() - start_time)

start_time = time.time()
get_parallel()
print("duration:", time.time() - start_time)
