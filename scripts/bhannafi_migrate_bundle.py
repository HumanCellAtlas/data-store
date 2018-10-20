#!/usr/bin/env python
import json
import boto3
import argparse


s3 = boto3.client('s3')


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("from_bucket")
parser.add_argument("to_bucket")
parser.add_argument("bundle_fqid")
args = parser.parse_args()


def copy_object(key):
    copy_source = {
        'Bucket': args.from_bucket,
        'Key': key
    }
    s3.copy(copy_source, args.to_bucket, key)

manifest = json.loads(s3.get_object(
    Bucket=args.from_bucket,
    Key=f"bundles/{args.bundle_fqid}"
)['Body'].read())

for file in manifest['files']:
    copy_object(f"blobs/{file['sha256']}.{file['sha1']}.{file['s3-etag']}.{file['crc32c']}")
    copy_object(f"files/{file['uuid']}.{file['version']}")
copy_object(f"bundles/{args.bundle_fqid}")
