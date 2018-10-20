#!/usr/bin/env python
"""
This script checks for missing objects, or mis-sized blobs, between a source replica
and a destination replica.
"""
import os
import sys
import argparse
import itertools
from concurrent.futures import ThreadPoolExecutor, as_completed

from bhannafi_utils import ReplicatedPair


PARALLEL_FACTOR=40


def check_size(rp: ReplicatedPair, key):
    if key.startswith("blobs"):
        if not rp.sizes_match(key):
            print(f"size_mismatch {key}")


def process_prefix(rp: ReplicatedPair, pfx):
    missing = dict()
    for a, b in itertools.zip_longest(rp.list_src(pfx), rp.list_dst(pfx)):
        if a is not None:
            try:
                del missing[a]
            except KeyError:
                missing[a] = rp.dst_replica

        if b is not None:
            try:
                del missing[b]
            except KeyError:
                missing[b] = rp.src_replica

    for key in missing:
        print(missing[key], key)


def verify_with_prefixes(rp: ReplicatedPair, prefixes):
    with ThreadPoolExecutor(PARALLEL_FACTOR) as executor:
        futures = [executor.submit(process_prefix, rp, pfx)
                   for pfx in prefixes]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(e)
            
    
def scan_bucket(rp: ReplicatedPair):
    digits = "0123456789abcdef"
    prefixes = [f"{kind}/{c}"
                for kind in ["blobs", "files", "bundles", "collections"]
                for c in digits]
    verify_with_prefixes(rp, prefixes)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("src_replica", choices=["aws", "gcp"])
    parser.add_argument("dst_replica", choices=["aws", "gcp"])
    parser.add_argument("key_or_file", nargs="?", default=None)
    parser.add_argument("-d", "--stage", default="dev", choices=["dev", "integration", "staging", "prod"])

    # xbrianh: workaround for https://bugs.python.org/issue15112
    args, remainder = parser.parse_known_args()
    if remainder:
        if 1 != len(remainder):
            parser.print_help()
            sys.exit(1)
        args.key_or_file = remainder[0]

    assert args.src_replica != args.dst_replica

    rp = ReplicatedPair(args.stage, args.src_replica, args.dst_replica)

    if args.key_or_file is None:
        scan_bucket(rp)
    elif os.path.isfile(args.key_or_file):
        with open(args.key_or_file, "r") as fh:
            prefixes = fh.read().split()
        verify_with_prefixes(rp, prefixes)
    else:
        process_prefix(rp, args.key_or_file)
