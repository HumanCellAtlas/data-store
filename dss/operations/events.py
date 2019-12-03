"""
JMESPath eventing and subscription admin tooling
"""
import typing
import argparse
import json
import logging
import traceback
from uuid import uuid4
from string import hexdigits
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from flashflood import FlashFlood, JournalID
from dcplib.aws.sqs import SQSMessenger
from dcplib.aws.clients import logs

from dss.config import Config, Replica
from dss.util.aws import resources
from dss.operations import dispatch
from dss.events import (get_bundle_metadata_document, record_event_for_bundle, journal_flashflood,
                        update_flashflood, list_new_flashflood_journals)
from dss.storage.bundles import Living
from dss.operations.util import monitor_logs, command_queue_url


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
    cmd_template = (f"events record --job-id {job_id} "
                    f"--prefix {args.prefix} "
                    f"--replica {replica.name} "
                    f"--keys {{keys}}")

    if args.keys is None:
        start_time = datetime.now()

        def forward_keys(bundle_fqids):
            with SQSMessenger(command_queue_url) as sqsm:
                for fqid in bundle_fqids:
                    sqsm.send(cmd_template.format(keys=f"bundles/{fqid}"))

        handle = Config.get_blobstore_handle(replica)
        with ThreadPoolExecutor(max_workers=4) as e:
            for c in set(hexdigits.lower()):
                bundle_fqids = Living(handle.list_v2(replica.bucket, f"bundles/{c}"))
                e.submit(forward_keys, bundle_fqids)
        monitor_logs(logs, job_id, start_time)
    else:
        for key in args.keys:
            msg = json.dumps(dict(action="record event", job_id=job_id, replica=replica.name, key=key))
            record_event_for_bundle(Replica[args.replica], key, (args.prefix,), use_version_for_timestamp=True)
            print(msg)

@events.action("journal",
               arguments={"--prefix": dict(required=True,
                                           help="flashflood prefix to journal events"),
                          "--number-of-events": dict(default=None, type=int),
                          "--starting-journal-id": dict(default=None),
                          "--job-id": dict(default=None)})
def journal(argv: typing.List[str], args: argparse.Namespace):
    """
    Compile flashflood event journals. If `--starting-journal-id` is not provided, journal contents are
    determined according to `--number-of-events` and queued into SQS for processing on AWS Lambda.
    Otherwise, execute the command is executed.
    """
    job_id = args.job_id or f"{uuid4()}"
    cmd_template = (f"events journal --job-id {job_id} "
                    f"--prefix {args.prefix} "
                    f"--number-of-events {args.number_of_events} "
                    f"--starting-journal-id {{starting_journal_id}}")

    if args.starting_journal_id is None:
        start_time = datetime.now()
        with SQSMessenger(command_queue_url) as sqsm:
            journals = list()
            for journal_id in list_new_flashflood_journals(args.prefix):
                journals.append(journal_id)
                if args.number_of_events == len(journals):
                    print(f"Journaling from {journals[0]} with {args.number_of_events} events.")
                    sqsm.send(cmd_template.format(starting_journal_id=journals[0]))
                    journals = list()
        monitor_logs(logs, job_id, start_time)
    else:
        msg = dict(action="journal", job_id=job_id, prefix=args.prefix, key=args.starting_journal_id)
        try:
            journal_flashflood(args.prefix, args.number_of_events, JournalID(args.starting_journal_id))
        except Exception:
            msg['ERROR'] = traceback.format_exc()
        print(json.dumps(msg))

@events.action("update",
               arguments={"--prefix": dict(required=True,
                                           help="flashflood prefix to journal events"),
                          "--number-of-updates-to-apply": dict(default=None, type=int)})
def update(argv: typing.List[str], args: argparse.Namespace):
    """
    Updates and deletions requests flashflood event data are recorded but not applied until `ff.update()` is called.
    This commmand causes flashflood to apply any available updates or deletions.
    """
    update_flashflood(args.prefix, args.number_of_updates_to_apply)

@events.action("list-journals",
               arguments={"--prefix": dict(required=True,
                                           help="flashflood prefix to journal events")})
def list_journals(argv: typing.List[str], args: argparse.Namespace):
    ff = FlashFlood(resources.s3, Config.get_flashflood_bucket(), args.prefix)
    for journal_id in ff.list_journals():
        print(journal_id)

@events.action("destroy",
               arguments={"--prefix": dict(required=True)})
def destroy(argv: typing.List[str], args: argparse.Namespace):
    """
    Delete all events recorded or journaled into `prefix`
    """
    if "yes" == input(f"Are you sure you want to delete all events for {args.prefix}? Enter 'yes' to continue: "):
        FlashFlood(resources.s3, Config.get_flashflood_bucket(), args.prefix)._destroy()
