#!/usr/bin/env python
"""
"""
import os
import json
import argparse
from cloud_blobstore import BlobNotFoundError
from concurrent.futures import ThreadPoolExecutor, as_completed

from bhannafi_utils import get_bucket, get_handle

def process_key(handle, bucket, key):
    for key in handle.list(bucket, key):
        print(key)
        return

def process_keys(handle, bucket, keys):
    with ThreadPoolExecutor(10) as executor:
        futures = [executor.submit(process_key, handle, bucket, key)
                   for key in keys]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(e)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("replica", choices=["aws", "gcp"])
    parser.add_argument("key_or_file")
    parser.add_argument("-d", "--stage", default="dev", choices=["dev", "integration", "staging", "prod"])
    args = parser.parse_args()

    handle = get_handle(args.replica)
    bucket = get_bucket(args.stage, args.replica)

    if os.path.isfile(args.key_or_file):
        with open(args.key_or_file, "r") as fh:
            keys = fh.read().split()
        process_keys(handle, bucket, keys)
    else:
        process_key(handle, bucket, args.key_or_file)
