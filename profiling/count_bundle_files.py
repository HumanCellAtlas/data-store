#! /usr/bin/env python

import sys
import datetime
import time
import argparse

import utils

deployment = "dev"
_dss_client = utils.get_dss_client(deployment)

def get_bundle(uuid, version):
    resp_obj = _dss_client.get_bundle(
        replica="aws",
        uuid=uuid,
        version=version,
    )
    return resp_obj['bundle']

def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--chunk-size", type=int, default=1000)
    parser.add_argument("--number-of-chunks", type=int, default=10)
    args = parser.parse_args()
    return parser.parse_args()
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("uuid")
    parser.add_argument("version")
    args = parser.parse_args()
    resp = get_bundle(args.uuid, args.version)
    assert resp['version'] == args.version
    number_of_files = len([f for f in _dss_client.get_bundle.iterate(replica="aws", uuid=args.uuid, version=args.version)])
    print(number_of_files)
