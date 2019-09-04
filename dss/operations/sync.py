"""
Replication consistency checks: verify and repair synchronization across replicas
"""
import argparse
import datetime
import itertools
import json
import logging
import os
import typing

from collections import namedtuple
from cloud_blobstore import BlobMetadataField, BlobNotFoundError

from dss import Config, Replica
from dcplib.aws.sqs import SQSMessenger, get_queue_url
from dss.storage.hcablobstore import compose_blob_key
from dss.operations import dispatch
from dss.storage.identifiers import BLOB_PREFIX, FILE_PREFIX, BUNDLE_PREFIX
from dss.util.version import datetime_from_timestamp


logger = logging.getLogger(__name__)
sync = dispatch.target("sync", help=__doc__)
ReplicationAnomaly = namedtuple("ReplicationAnomaly", "key anomaly")


def _log_warning(**kwargs):
    logger.warning(json.dumps(kwargs))


@sync.action('verify-sync-all',
             arguments={"--source-replica": {'choices': [r.name for r in Replica], 'required': True},
                        "--destination-replica": {'choices': [r.name for r in Replica], 'required': True},
                        "--since": {'required': False, 'help': "Only check objects newer than this (DSS_VERSION)"
                                                               " e.g. 1970-01-01T000000.000000"}})
def verify_sync_all(argv: typing.List[str], args: argparse.Namespace):
    """
    Like verify-sync, but provides keys from the designated source replica
    (so that keys don't need to be supplied individually).
    """
    def list_objects_since(replica, since: datetime.datetime):
        replica = Replica[replica]
        handle = Config.get_blobstore_handle(replica)
        for prefix in (BUNDLE_PREFIX, FILE_PREFIX, BLOB_PREFIX):
            for name, metadata in handle.list_v2(replica.bucket, prefix=prefix):
                if metadata[BlobMetadataField.LAST_MODIFIED] > since:
                    yield name

    def _chunk(it, size):
        """Split an iterable into chunks of `size` (https://stackoverflow.com/a/22045226)"""
        return iter(lambda: tuple(itertools.islice(iter(it), size)), ())

    arbitrary_small_date = datetime_from_timestamp("1970-01-01T000000.000000Z")
    cutoff = datetime_from_timestamp(args.since) if args.since else arbitrary_small_date
    objects_to_check = list_objects_since(args.source_replica, cutoff)
    for chunk in _chunk(objects_to_check, 1024):
        # To call :func:`verify_sync`, we need to perform some argument parsing
        # and simulate command-line invocation.
        _args = [
            'sync', 'verify-sync',
            '--source-replica', args.source_replica,
            '--destination-replica', args.destination_replica,
            '--keys', *chunk
        ]
        verify_sync(_args, dispatch.parser.parse_args(_args))


@sync.action("verify-sync",
             arguments={"--source-replica": dict(choices=[r.name for r in Replica], required=True),
                        "--destination-replica": dict(choices=[r.name for r in Replica], required=True),
                        "--keys": dict(default=None, nargs="*", help="keys to check.")})
def verify_sync(argv: typing.List[str], args: argparse.Namespace):
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


@sync.action("trigger-sync",
             arguments={"--source-replica": dict(choices=[r.name for r in Replica], required=True),
                        "--destination-replica": dict(choices=[r.name for r in Replica], required=True),
                        "--keys": dict(default=None, nargs="*", help="keys to check.")})
def trigger_sync(argv: typing.List[str], args: argparse.Namespace):
    """
    Invoke the sync daemon on a set of keys via sqs.
    """
    sync_queue_url = get_queue_url("dss-sync-operation-" + os.environ['DSS_DEPLOYMENT_STAGE'])
    with SQSMessenger(sync_queue_url) as sqsm:
        for key in args.keys:
            msg = json.dumps(dict(source_replica=args.source_replica,
                                  dest_replica=args.destination_replica,
                                  key=key))
            sqsm.send(msg)


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
