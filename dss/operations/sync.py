"""
Replication consistency checks: verify and repair synchronization across replicas
"""
import os
import json
import typing
import logging
import argparse

from collections import namedtuple
from cloud_blobstore import BlobNotFoundError

from dss import Config, Replica
from dss.sqs import MessageQueuer, get_queue_url
from dss.storage.hcablobstore import compose_blob_key
from dss.operations import dispatch
from dss.storage.identifiers import BLOB_PREFIX, FILE_PREFIX, BUNDLE_PREFIX


logger = logging.getLogger(__name__)
sync = dispatch.target("sync", help=__doc__)
ReplicationAnomaly = namedtuple("ReplicationAnomaly", "key anomaly")


def _log_warning(**kwargs):
    logger.warning(json.dumps(**kwargs))


@sync.action("verify-entity-replication",
             arguments={"--source-replica": dict(choices=[r.name for r in Replica], required=True),
                        "--destination-replica": dict(choices=[r.name for r in Replica], required=True),
                        "--keys": dict(default=None, nargs="*", help="keys to check.")})
def verify_entity_replication(argv: typing.List[str], args: argparse.Namespace):
    """
    Verify replication for a DSS entity, following references.
    for example, if a bundle key is provided, replication will be verified for the bundle and all referenced files and
    blobs.
    """
    assert args.source_replica != args.destination_replica
    src_replica = Replica[args.source_replica]
    dst_replica = Replica[args.destination_replica]
    src_handle = Config.get_blobstore_handle(src_replica)
    dst_handle = Config.get_blobstore_handle(dst_replica)

    for key in args.keys:
        if key.startswith(BUNDLE_PREFIX):
            verify = verify_bundle_replication
        elif key.startswith(FILE_PREFIX):
            verify = verify_file_replication
        elif key.startswith(BLOB_PREFIX):
            verify = verify_blob_replication
        else:
            raise ValueError(f"cannot handle key {key}")
        for anomaly in verify(src_handle, dst_handle, src_replica.bucket, dst_replica.bucket, key):
            _log_warning(ReplicationAnomaly=dict(key=anomaly.key, anomaly=anomaly.anomaly))


@sync.action("sync",
             arguments={"--source-replica": dict(choices=[r.name for r in Replica], required=True),
                        "--destination-replica": dict(choices=[r.name for r in Replica], required=True),
                        "--keys": dict(default=None, nargs="*", help="keys to check.")})
def trigger_sync(argv: typing.List[str], args: argparse.Namespace):
    sync_queue_url = get_queue_url("dss-sync-operation-" + os.environ['DSS_DEPLOYMENT_STAGE'])
    with MessageQueuer(sync_queue_url) as mq:
        for key in args.keys:
            msg = json.dumps(dict(source_replica=args.source_replica,
                                  dest_replica=args.destination_replica,
                                  key=key))
            mq.send(msg)

def verify_blob_replication(src_handle, dst_handle, src_bucket, dst_bucket, key):
    """
    Return list of ReplicationAnomaly for blobs
    """
    anomalies = list()
    size = src_handle.get_size(src_bucket, key)
    try:
        target_size = dst_handle.get_size(dst_bucket, key)
    except BlobNotFoundError:
        anomalies.append(ReplicationAnomaly(key=key, anomaly="missing on target replica"))
    else:
        if size != target_size:
            anomalies.append(ReplicationAnomaly(key=key, anomaly=f"blob size mismatch: {size} {target_size}"))
    return anomalies

def verify_file_replication(src_handle, dst_handle, src_bucket, dst_bucket, key):
    """
    Return list of ReplicationAnomaly for files+blobs
    """
    anomalies = list()
    try:
        file_metadata = json.loads(src_handle.get(src_bucket, key))
    except BlobNotFoundError:
        anomalies.append(ReplicationAnomaly(key=key, anomaly="missing on source replica"))
    else:
        try:
            target_file_metadata = json.loads(dst_handle.get(dst_bucket, key))
        except BlobNotFoundError:
            anomalies.append(ReplicationAnomaly(key=key, anomaly="missing on target replica"))
        else:
            if file_metadata != target_file_metadata:
                anomalies.append(ReplicationAnomaly(key=key, anomaly="file metadata mismatch"))
        blob_key = compose_blob_key(file_metadata)
        anomalies.extend(verify_blob_replication(src_handle, dst_handle, src_bucket, dst_bucket, blob_key))
    return anomalies

def verify_bundle_replication(src_handle, dst_handle, src_bucket, dst_bucket, key):
    """
    Return list of ReplicationAnomaly for bundles+files+blobs
    """
    anomalies = list()
    try:
        bundle_metadata = json.loads(src_handle.get(src_bucket, key))
    except BlobNotFoundError:
        anomalies.append(ReplicationAnomaly(key=key, anomaly="missing on source replica"))
    else:
        try:
            target_bundle_metadata = json.loads(dst_handle.get(dst_bucket, key))
        except BlobNotFoundError:
            anomalies.append(ReplicationAnomaly(key=key, anomaly="missing on target replica"))
        else:
            if bundle_metadata != target_bundle_metadata:
                anomalies.append(ReplicationAnomaly(key=key, anomaly="bundle metadata mismatch"))
        for file_ in bundle_metadata['files']:
            file_key = "{}/{}.{}".format(FILE_PREFIX, file_['uuid'], file_['version'])
            anomalies.extend(verify_file_replication(src_handle, dst_handle, src_bucket, dst_bucket, file_key))
    return anomalies
