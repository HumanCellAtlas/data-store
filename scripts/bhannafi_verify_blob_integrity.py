#!/usr/bin/env python
import os
import sys
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

from bhannafi_utils import get_bucket, get_handle

def verify(handle, bucket, key):
    if not key.startswith("blobs/"):
        raise Exception("Expected a blob key")

    etag = key.split(".")[2]
    obj_checksum = handle.get_cloud_checksum(bucket, key) 
    if etag != obj_checksum:
        print(key, etag, obj_checksum)
        sys.stdin.flush()

def verify_with_keys(handle, bucket, keys):
    with ThreadPoolExecutor(10) as executor:
        executor.map(lambda key: verify(handle, bucket, key), keys)

def scan_bucket(handle, bucket):
    digits = "0123456789abcdef"
    prefixes = [f"blobs/{c}" for c in digits]

    def process_prefix(pfx):
        for key in handle.list(bucket, pfx):
            verify(handle, bucket, key)

    with ThreadPoolExecutor(10) as executor:
        executor.map(process_prefix, prefixes)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("replica", choices=["aws", "gcp"])
    parser.add_argument("key_or_file", nargs="?", default=None)
    parser.add_argument("-d", "--stage", default="dev", choices=["dev", "integration", "staging", "prod"])

    # xbrianh: workaround for https://bugs.python.org/issue15112
    args, remainder = parser.parse_known_args()
    if remainder:
        if 1 != len(remainder):
            parser.print_help()
            sys.exit(1)
        args.key_or_file = remainder[0]

    handle = get_handle(args.replica)
    bucket = get_bucket(args.stage, args.replica)

    if args.key_or_file is None:
        scan_bucket(handle, bucket)
    elif os.path.isfile(args.key_or_file):
        with open(args.key_or_file, "r") as fh:
            keys = [line.strip() for line in fh]
        verify_with_keys(handle, bucket, keys)
    else:
        verify(handle, bucket, args.key_or_file)
