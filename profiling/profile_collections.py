#! /usr/bin/env python

import datetime
import time
from uuid import uuid4
import argparse

import utils

deployment = "dev"

_handle = utils.get_handle("aws")
_bucket = utils.get_bucket(deployment)
_dss_client = utils.get_dss_client(deployment)

def get_content_chunks(contents_type="file", chunk_size=200):
    for keys in utils.list_chunks(_handle, _bucket, f"{contents_type}s", chunk_size):
        items = list()
        for key in keys:
            _, fqid = key.split("/")
            uuid, version = fqid.split(".", 1)
            items.append({
                "type": contents_type,
                "uuid": uuid,
                "version": version,
            })
        yield items

def get_collection(uuid, version):
    resp_obj = _dss_client.get_collection(
        replica="aws",
        uuid=uuid,
        version=version,
    )
    return resp_obj

def create_collection(contents, uuid=None, version=None):
    resp_obj = _dss_client.put_collection(
        replica="aws",
        name="frank",
        description="george",
        details=dict(),
        uuid=uuid,
        version=version,
        contents=contents,
    )
    return resp_obj

def patch_collection(contents, uuid, version):
    resp_obj = _dss_client.patch_collection(
        replica="aws",
        name="frank",
        description="george",
        details=dict(),
        uuid=uuid,
        version=version,
        add_contents=contents,
    )
    return resp_obj

def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--chunk-size", type=int, default=1000)
    parser.add_argument("--number-of-chunks", type=int, default=10)
    args = parser.parse_args()
    return parser.parse_args()
    
if __name__ == "__main__":
    args = _parse_args()
    uuid = str(uuid4())
    version = str(datetime.datetime.utcnow().strftime("%Y-%m-%dT%H%M%S.%fZ"))
    resp = create_collection([], uuid, version)
    for i, c in enumerate(get_content_chunks(chunk_size=args.chunk_size)):
        version = resp['version']
        resp = get_collection(uuid, version)
        content_length = len(resp['contents'])
        start_time = time.time()
        resp = patch_collection(c, uuid, version)
        duration = time.time() - start_time
        print(uuid, "size before patch:", content_length, "patch duration:", duration)
        if i == args.number_of_chunks - 1:
            break
