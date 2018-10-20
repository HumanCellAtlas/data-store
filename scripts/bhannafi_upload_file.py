#!/usr/bin/env python
import os
import sys
import json
import boto3
import argparse
import mimetypes
from boto3.s3.transfer import TransferConfig

from dcplib import s3_multipart
from dcplib.checksumming_io import ChecksummingBufferedReader

def encode_tags(tags):
    return [dict(Key=k, Value=v) for k, v in tags.items()]

def _mime_type(filename):
    type_, encoding = mimetypes.guess_type(filename)
    if encoding:
        return encoding
    if type_:
        return type_
    return "application/octet-stream"

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("local_path")
parser.add_argument("remote_path")
parser.add_argument("--multipart-size", default=None)
args = parser.parse_args()

s3 = boto3.resource("s3")
bucket_name, key = args.remote_path[5:].split("/", 1)
bucket = s3.Bucket(bucket_name)

if args.multipart_size is None:
    multipart_chunksize = s3_multipart.get_s3_multipart_chunk_size(
        os.path.getsize(args.local_path)
    )
else:
    multipart_chunksize = args.multipart_size

with open(args.local_path, "rb") as raw_fh:
    with ChecksummingBufferedReader(raw_fh, multipart_chunksize) as fh:
        bucket.upload_fileobj(
            fh,
            key,
            Config=TransferConfig(
                multipart_threshold=s3_multipart.MULTIPART_THRESHOLD,
                multipart_chunksize=multipart_chunksize
            ),
            ExtraArgs={
                'ContentType': _mime_type(fh.raw.name),
            }
        )
        sums = fh.get_checksums()
        metadata = {
            "hca-dss-s3_etag": sums["s3_etag"],
            "hca-dss-sha1": sums["sha1"],
            "hca-dss-sha256": sums["sha256"],
            "hca-dss-crc32c": sums["crc32c"],
        }
    
        s3.meta.client.put_object_tagging(
            Bucket=bucket.name,
            Key=key,
            Tagging=dict(
                TagSet=encode_tags(metadata)
            )
        )
