#! /usr/bin/env python

import sys
import datetime
import time
from uuid import uuid4
import argparse

import utils

deployment = "dev"

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
    keys = [
        "0185a926-1b58-47fa-a4bd-acceb090ef5c.2018-11-12T235834.225377Z",
        "039b1f97-850a-4e10-985c-31677bb04b11.2018-11-12T235834.225377Z",
        "17c93feb-45ec-4d8f-bb94-d4d35bf2f7a1.2018-11-12T235834.225377Z",
        "4ab87882-2760-4b93-b6cd-0fc80e3da8fd.2018-11-12T235834.225377Z",
        "4ed6abd5-08e3-4f80-925f-fb7fe7c5ac1a.2018-11-12T235854.981860Z",
        "689cb552-d86a-4d0d-ae19-4ed77174c486.2018-11-12T235854.981860Z",
        "6b30dd8a-6602-4ecf-9fc6-2fe4c9c3276c.2018-11-12T235854.981860Z",
        "6dba9fc6-74b0-4997-8caa-06e98abbb75b.2018-11-12T235854.981860Z",
        "741b852c-4bba-47e7-b265-8f977f280ef6.2018-11-12T235854.981860Z",
        "863f2695-351d-429f-97a5-635ac83cc506.2018-11-12T235834.225377Z",
        "92474d62-8b21-4a5b-94d6-7b2e06376049.2018-11-12T235854.981860Z",
        "969ac309-c345-47d8-9e8b-69353e53a968.2018-11-12T235854.981860Z",
        "975c44b9-b689-4d89-85e3-d034d7b9e5bb.2018-11-12T235834.225377Z",
        "98551dc3-ae16-4646-b388-44c2477fad3f.2018-11-12T235834.225377Z",
        "98e9e3b0-e2b3-4a48-9814-641e75bb8c18.2018-11-12T235854.981860Z",
        "edd8fe1c-3edc-468f-bd72-5ea41b42b80c.2018-11-12T235834.225377Z",
    ]
    i = 0
    items = list()
    while True:
        fqid = keys[i]
        i = (i+1) % len(keys)
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
        except:
            if not tries:
                raise
            print("------> retrying", uuid, version)
            get_bundle(uuid, version)  # warm up the cash
            sys.stdout.flush()
            tries -= 1

def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("uuid", type=str)
    parser.add_argument("version", type=str)
    parser.add_argument("--patch-size", type=int, default=1000)
    parser.add_argument("--number-of-patches", type=int, default=3)
    args = parser.parse_args()
    return parser.parse_args()
                   
if __name__ == "__main__":
    args = _parse_args()
    # uuid = str(uuid4())
    # version = str(datetime.datetime.utcnow().strftime("%Y-%m-%dT%H%M%S.%fZ"))
    # resp = create_bundle(uuid, version)
    uuid = args.uuid
    version = args.version
    resp = get_bundle(uuid, version)
    # resp = dict(uuid=uuid, version=version)
    for i, c in enumerate(get_files_chunks(patch_size=args.patch_size)):
        version = resp['version']
        # number_of_files = len([f for f in _dss_client.get_bundle.iterate(replica="aws", uuid=uuid, version=version)])
        start_time = time.time()
        resp = patch_bundle(c, uuid, version)
        duration = time.time() - start_time
        # print(uuid, version, "number of files:", number_of_files, "patch duration:", duration)
        print(uuid, version, "patch duration:", duration)
        sys.stdout.flush()
        if i == (args.number_of_patches - 1):
            break
