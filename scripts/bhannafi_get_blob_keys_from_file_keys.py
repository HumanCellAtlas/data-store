#!/usr/bin/env python
import os
import json
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

from bhannafi_utils import get_bucket, get_handle

def ref_check_file(handle, bucket, key):
    data = json.loads(handle.get(bucket, key))
    blob_key = f"blobs/{data['sha256']}.{data['sha1']}.{data['s3-etag']}.{data['crc32c']}"
    print(blob_key)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("replica", choices=["aws", "gcp"])
    parser.add_argument("key")
    parser.add_argument("-d", "--stage", default="dev", choices=["dev", "integration", "staging", "prod"])
    args = parser.parse_args()

    handle = get_handle(args.replica)
    bucket = get_bucket(args.stage, args.replica)

    if os.path.isfile(args.key):
        with open(args.key, "r") as fh:
            with ThreadPoolExecutor(40) as executor:
                for line in fh:
                    executor.submit(ref_check_file, handle, bucket, line.strip())
    else:
        ref_check_file(handle, bucket, args.key)
