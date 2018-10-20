#!/usr/bin/env python
"""
Invoke daemons/dss-sync-sfn directly
usage {script} {src_replica} {dst_replica} {key}
key may either be an object key with format:
    {kind}/{fqid}
or path to a text file of column-formatted keys

sync repairs should be carried out according to referential hierarchy
    1. blobs
    2. files
    3. bundles
    4. collections
"""
import os
import uuid
import json
import argparse

from bhannafi_utils import clients, get_bucket, get_handle


account_id = clients.sts.get_caller_identity()['Account']


def start_sync_sfn(stage, src_replica, dst_replica, key, size):
    state = {
        'source_replica': src_replica,
        'dest_replica': dst_replica,
        'source_key': key,
        'source_obj_metadata': {
            'size': size,
        }
    }
    
    state_machine_arn = f"arn:aws:states:us-east-1:{account_id}:stateMachine:dss-sync-sfn-{stage}"
    
    return clients.stepfunctions.start_execution(
        stateMachineArn=state_machine_arn,
        name=str(uuid.uuid1()),
        input=json.dumps(state),
    )


def sync_obj(stage, src_replica, dst_replica, key):
    handle = get_handle(src_replica)
    bucket = get_bucket(stage, src_replica)
    size = handle.get_size(bucket, key)
    start_sync_sfn(stage, src_replica, dst_replica, key, size)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("src_replica", choices=["aws", "gcp"])
    parser.add_argument("dst_replica", choices=["aws", "gcp"])
    parser.add_argument("-d", "--stage", default="dev", choices=["dev", "integration", "staging", "prod"])
    parser.add_argument("key")
    args = parser.parse_args()

    assert args.src_replica != args.dst_replica

    if os.path.isfile(args.key):
        with open(args.key, "r") as fh:
            for line in fh:
                key = line.strip()
                sync_obj(args.stage, args.src_replica, args.dst_replica, key)
    else:
        sync_obj(args.stage, args.src_replica, args.dst_replica, args.key)
