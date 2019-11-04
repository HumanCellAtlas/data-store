import os
import sys
import logging
from itertools import cycle

import domovoi

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


class ReplicaStatus:
    prefix = None
    finished_journaling = False
    finished_updating = False

    def __init__(self, prefix):
        self.prefix = prefix


@app.scheduled_function("rate(5 minutes)")
def flashflood_journal_and_update(event, context):
    # TODO: Make this configurable
    minimum_number_of_events = 10 if "dev" == os.environ['DSS_DEPLOYMENT_STAGE'] else 1000

    def lambda_seconds_remaining() -> float:
        # lambda time to live is configured with `lambda_timeout` in `daemons/dss-events-scribe/.chalice/config.json`
        return context.get_remaining_time_in_millis() / 1000

    replicas = [ReplicaStatus(r.flashflood_prefix_read) for r in Replica]

    for replica in cycle(replicas):
        if lambda_seconds_remaining() > 120 and not all(replica.finished_journaling for replica in replicas):
            if not replica.finished_journaling:
                did_journal = journal_flashflood(replica.prefix, minimum_number_of_events)
                if did_journal:
                    logger.info(f"Compiled event journal with {minimum_number_of_events} events "
                                f"for flashflood prefix {replica.prefix}")
                else:
                    logger.info(f"Finished compiling event journals for flashflood prefix {replica.prefix}")
                    replica.finished_journaling = True
        else:
            break

    for replica in cycle(replicas):
        if lambda_seconds_remaining() > 30 and not all(replica.finished_updating for replica in replicas):
            if not replica.finished_updating:
                number_of_updates_applied = update_flashflood(replica.prefix, minimum_number_of_events)
                if 0 < number_of_updates_applied:
                    logger.info(f"Applied {number_of_updates_applied} event updates or deletes "
                                f"for flashflood prefix {replica.prefix}")
                else:
                    logger.info(f"Finished applying event updates for flashflood prefix {replica.prefix}")
                    replica.finished_updating = True
        else:
            break
