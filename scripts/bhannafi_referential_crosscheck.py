#!/usr/bin/env python
import os
import json
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

from bhannafi_utils import ReplicatedPair

def exists(handle, bucket, key):
    for key in handle.list(bucket, key):
        return True
    print(f"missing {key}")
    return False

def check_blob(handle, bucket, key, size):
    if not exists(handle, bucket, key):
        return
    if not size == handle.get_size(bucket, key):
        print(f"size_mismatch {key}")

def check_file(handle, bucket, key):
    if not exists(handle, bucket, key):
        return
    try:
        data = json.loads(handle.get(bucket, key))
    except json.decoder.JSONDecodeError:
        print(f"malformed {key}")
        return
    blob_key = f"blobs/{data['sha256']}.{data['sha1']}.{data['s3-etag']}.{data['crc32c']}"
    check_blob(handle, bucket, blob_key, data['size'])

def ref_check_file(rp, key):
    data = json.loads(rp.src_handle.get(rp.src_bucket, key))
    blob_key = f"blobs/{data['sha256']}.{data['sha1']}.{data['s3-etag']}.{data['crc32c']}"
    check_blob(rp.dst_handle, rp.dst_bucket, blob_key, data['size'])

def ref_check_bundle(rp, key):
    manifest = json.loads(rp.src_handle.get(rp.src_bucket, key))
    for file in manifest['files']:
        file_key = f"files/{file['uuid']}.{file['version']}"
        check_file(rp.dst_handle, rp.dst_bucket, file_key)

def ref_check_keys(rp, keys):
    with ThreadPoolExecutor(max_workers=40) as executor:
        for key in keys:
            if key.startswith("bundles"):
                executor.submit(ref_check_bundle, rp, key)
            elif key.startswith("files"):
                executor.submit(ref_check_file, rp, key)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("src_replica", choices=["aws", "gcp"])
    parser.add_argument("dst_replica", choices=["aws", "gcp"])
    parser.add_argument("key")
    parser.add_argument("-d", "--stage", default="dev", choices=["dev", "integration", "staging", "prod"])
    args = parser.parse_args()

    rp = ReplicatedPair(args.stage, args.src_replica, args.dst_replica)

    if os.path.isfile(args.key):
        with open(args.key, "r") as fh:
            keys = [line.strip() for line in fh]
        ref_check_keys(rp, keys)
    elif args.key.startswith("bundle"):
        ref_check_bundle(rp, args.key)
    elif args.key.startswith("file"):
        ref_check_file(rp, args.key)
    else:
        raise Exception(f"Cannot check references for {args.key}")
