"""
See Readme.md in this directory for documentation on the dss-notify-v2 daemon.

storage_event -> invoke_notify_daemon -> invoke_sfn -> sfn_dynamodb_loop -> sqs -> invoke_notify_daemon
"""

import os
import sys
import logging
import typing
import time
from itertools import cycle

import domovoi
from flashflood import FlashFloodJournalingError

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss import Config, Replica
from dss.logging import configure_lambda_logging
from dss.events import journal_flashflood, update_flashflood

from dss.util import countdown


configure_lambda_logging()
logger = logging.getLogger(__name__)
dss.Config.set_config(dss.BucketConfig.NORMAL)

app = domovoi.Domovoi()


@app.scheduled_function("rate(5 minutes)")
def flashflood_journal_and_update(event, context):
    # TODO: Make this configurable
    minimum_number_of_events = 10 if "dev" == os.environ['DSS_DEPLOYMENT_STAGE'] else 1000

    class PrefixStatus(str):
        is_journaled = False
        is_updated = False

    def lambda_seconds_remaining():
        return context.get_remaining_time_in_millis() / 1000 - 30

    prefixes = MutableCycle([r.flashflood_prefix_read for r in Replica])
    for seconds_remaining, pfx in zip(countdown(lambda_seconds_remaining()), prefixes):
        if not journal_flashflood(pfx, minimum_number_of_events):
            prefixes.remove(pfx)

    prefixes = MutableCycle([r.flashflood_prefix_read for r in Replica])
    for seconds_remaining, pfx in zip(countdown(lambda_seconds_remaining()), prefixes):
        if not update_flashflood(pfx):
            prefixes.remove(pfx)

class MutableCycle:
    """
    Safely cycle members of a mutable list.
    """
    def __init__(self, items: typing.List[typing.Any]):
        self.items = items.copy()
        self.index = -1

    def __iter__(self) -> typing.Iterator[typing.Any]:
        while self.items:
            self.index = (1 + self.index) % len(self.items)
            yield self.items[self.index]

    def remove(self, item):
        r = self.items.index(item)
        del self.items[r]
        if r <= self.index:
            self.index -= 1
