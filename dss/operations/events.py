"""
JMESPath eventing and subscription admin tooling
"""
import os
import typing
import argparse
import json
import logging
from uuid import uuid4
from datetime import datetime

from cloud_blobstore import BlobNotFoundError
from flashflood import FlashFlood
from dcplib.aws.sqs import SQSMessenger
from dcplib.aws.clients import logs

from dss.config import Config, Replica
from dss.util.aws import resources
from dss.operations import dispatch
from dss.events import get_bundle_metadata_document, record_event_for_bundle
from dss.events.handlers.notify_v2 import _versioned_tombstone_key_regex, _unversioned_tombstone_key_regex
from dss.storage.identifiers import TOMBSTONE_SUFFIX
from dss.operations.util import map_bucket, monitor_logs, command_queue_url


logger = logging.getLogger(__name__)


events = dispatch.target("events", help=__doc__)


@events.action("bundle-metadata-document",
               arguments={"--replica": dict(choices=[r.name for r in Replica], required=True),
                          "--keys": dict(required=True,
                                         nargs="*",
                                         help="bundle keys to generate documents.")})
def bundle_metadata_document(argv: typing.List[str], args: argparse.Namespace):
    replica = Replica[args.replica]
    for key in args.keys:
        md = get_bundle_metadata_document(replica, key)
        if md is not None:
            print(md)

@events.action("record",
               arguments={"--replica": dict(choices=[r.name for r in Replica],
                                            required=True,
                                            help="Source replica."),
                          "--prefix": dict(required=True,
                                           help="Destination flashflood prefix."),
                          "--keys": dict(default=None,
                                         nargs="*",
                                         help="Record events for these bundles."),
                          "--job-id": dict(default=None)})
def record(argv: typing.List[str], args: argparse.Namespace):
    """
    Record events for `keys` into flashflood prefix `prefix`
    If `keys` is omitted, record an event for each bundle in `replica` via lambda forwarding.
    """
    replica = Replica[args.replica]
    job_id = args.job_id or f"{uuid4()}"
    cmd_template = f"events record  --prefix {args.prefix} --replica {replica.name} --job-id {job_id} --keys {{}}"

    if args.keys is None:
        start_time = datetime.now()

        def forward_keys(keys):
            with SQSMessenger(command_queue_url) as sqsm:
                tombstone_id = None
                for key in keys:
                    if _versioned_tombstone_key_regex.match(key):
                        continue
                    elif _unversioned_tombstone_key_regex.match(key):
                        fqid = key.rsplit("/")[0]
                        tombstone_id = fqid.replace(f".{TOMBSTONE_SUFFIX}", "")
                        continue
                    else:
                        if tombstone_id and tombstone_id in key:
                            continue
                        else:
                            tombstone_id = None
                            sqsm.send(cmd_template.format(key))

        handle = Config.get_blobstore_handle(replica)
        map_bucket(forward_keys, handle, replica.bucket, f"bundles/")
        monitor_logs(logs, job_id, start_time)
    else:
        for key in args.keys:
            msg = json.dumps(dict(action="record event", job_id=job_id, replica=replica.name, key=key))
            record_event_for_bundle(Replica[args.replica], key, (args.prefix,))
            print(msg)

@events.action("journal",
               arguments={"--prefix": dict(required=True,
                                           help="flashflood prefix to journal events"),
                          "--minimum-number-of-events": dict(default=100),
                          "--minimum-size": dict(default=1024 * 1024)})
def journal(argv: typing.List[str], args: argparse.Namespace):
    ff = FlashFlood(resources.s3, Config.get_flashflood_bucket(), args.prefix)
    ff.journal(int(args.number_of_events))

@events.action("destroy",
               arguments={"--prefix": dict(required=True)})
def delete_all(argv: typing.List[str], args: argparse.Namespace):
    """
    Delete all events recorded or journaled into `prefix`
    """
    FlashFlood(resources.s3, Config.get_flashflood_bucket(), args.prefix)._destroy()
