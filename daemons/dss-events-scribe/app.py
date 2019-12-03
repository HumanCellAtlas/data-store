import os
import sys
import json
import logging

import domovoi

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss import Config, Replica
from dss.logging import configure_lambda_logging
from dss.events import journal_flashflood, update_flashflood


configure_lambda_logging()
logger = logging.getLogger(__name__)
dss.Config.set_config(dss.BucketConfig.NORMAL)

app = domovoi.Domovoi()


# TODO: Make this configurable
NUMBER_OF_EVENTS_PER_JOURNAL = 10 if "dev" == os.environ['DSS_DEPLOYMENT_STAGE'] else 1000


@app.sqs_queue_subscriber("dss-events-scribe-" + os.environ['DSS_DEPLOYMENT_STAGE'],
                          batch_size=1,
                          queue_attributes=dict(VisibilityTimeout="600"))
def handle_sqs_message(event, context):
    if 1 != len(event['Records']):
        logger.error(f"Received {len(event['Records'])}, expected 1")
    else:
        msg = json.loads(event['Records'][0]['body'])
        replica = Replica[msg['replica']]
        journal_or_update_events(replica, context)

def journal_or_update_events(replica: Replica, context):
    def lambda_seconds_remaining() -> float:
        # lambda time to live is configured with `lambda_timeout` in `daemons/dss-events-scribe/.chalice/config.json`
        return context.get_remaining_time_in_millis() / 1000

    prefix = replica.flashflood_prefix_read
    journal_id = journal_flashflood(prefix, NUMBER_OF_EVENTS_PER_JOURNAL)
    if journal_id is not None:
        logger.info(json.dumps(dict(message="Compiled new event journal",
                                    flashflood_prefix=prefix,
                                    journal_id=journal_id,
                                    number_of_events=NUMBER_OF_EVENTS_PER_JOURNAL), indent=4))
    else:
        # This branch indicates there were not enough new events to compile a journal
        # Instead, apply event updates until lambda expires
        update_batch_size = 5
        total_updates_applied = 0
        while 30 < lambda_seconds_remaining():
            number_of_updates_applied = update_flashflood(prefix, update_batch_size)
            if 0 == number_of_updates_applied:
                break
            total_updates_applied += number_of_updates_applied
        logger.info(json.dumps(dict(message="Applied event updates and deletes",
                                    flashflood_prefix=prefix,
                                    number_of_events=total_updates_applied), indent=4))
