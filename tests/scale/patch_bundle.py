#! /usr/bin/env python

import os
import sys
import datetime
import time
from uuid import uuid4
import argparse
import boto3
from cloud_blobstore.s3 import S3BlobStore

import utils

deployment = os.environ['DSS_DEPLOYMENT_STAGE']

_handle = utils.get_handle("aws")
_bucket = utils.get_bucket(deployment)
_dss_client = utils.get_dss_client(deployment)

def _get_files_chunks(contents_type="file", patch_size=200):
    for keys in utils.list_chunks(_handle, _bucket, f"{contents_type}s", patch_size):
        items = list()
        for key in keys:
            _, fqid = key.split("/")
            uuid, version = fqid.split(".", 1)
            items.append({
                "indexed": False,
                "name": str(uuid4()),
                "uuid": uuid,
                "version": version,
            })
        yield items

def get_files_chunks(contents_type="file", patch_size=200):
    handle = S3BlobStore(boto3.client("s3"))
    keys = list()
    for key in handle.list(os.environ['DSS_S3_BUCKET'], "files/"):
        keys.append(key.split("/")[1])
        if len(keys) >= 20:
            break
    print(keys)
    i = 0
    items = list()
    while True:
        fqid = keys[i]
        i = (i + 1) % len(keys)
        uuid, version = fqid.split(".", 1)
        items.append({
            "indexed": False,
            "name": str(uuid4()),
            "uuid": uuid,
            "version": version,
        })
        if patch_size == len(items):
            yield items
            items = list()

def get_bundle(uuid, version):
    resp_obj = _dss_client.get_bundle(
        replica="aws",
        uuid=uuid,
        version=version,
    )
    return resp_obj['bundle']

def create_bundle(uuid=None, version=None):
    resp_obj = _dss_client.put_bundle(
        replica="aws",
        uuid=uuid,
        version=version,
        files=list(),
        creator_uid=123,
    )
    return resp_obj

def patch_bundle(files, uuid, version):
    tries = 20
    while tries:
        try:
            return _dss_client.patch_bundle(
                replica="aws",
                uuid=uuid,
                version=version,
                add_files=files,
            )
        except Exception:
            if not tries:
                raise
            print("------> retrying", uuid, version)
            get_bundle(uuid, version)  # warm up the cash
            sys.stdout.flush()
            tries -= 1

def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--patch-size", type=int, default=1000)
    parser.add_argument("--number-of-patches", type=int, default=3)
    return parser.parse_args()

if __name__ == "__main__":
    args = _parse_args()
    uuid = str(uuid4())
    version = str(datetime.datetime.utcnow().strftime("%Y-%m-%dT%H%M%S.%fZ"))
    resp = create_bundle(uuid, version)
    number_of_files = 0
    for i, c in enumerate(get_files_chunks(patch_size=args.patch_size)):
        version = resp['version']
        start_time = time.time()
        resp = patch_bundle(c, uuid, version)
        number_of_files += args.patch_size
        duration = time.time() - start_time
        print(uuid, version, "number of files:", number_of_files, "patch duration:", duration)
        sys.stdout.flush()
        if i == (args.number_of_patches - 1):
            break
