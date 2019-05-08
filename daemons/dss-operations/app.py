"""
This Lambda executes commands forwarded from the DSS operations CLI
"""

import os
import sys
import logging

import domovoi

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss.logging import configure_lambda_logging
from dss.operations import dispatch

configure_lambda_logging()
logger = logging.getLogger(__name__)
dss.Config.set_config(dss.BucketConfig.NORMAL)

app = domovoi.Domovoi()

# Handle commands forwarded from the DSS Operations CLI
@app.sqs_queue_subscriber(
    "dss-operations-" + os.environ['DSS_DEPLOYMENT_STAGE'],
    queue_attributes=dict(
        VisibilityTimeout="3600",  # Retry every hour
        MessageRetentionPeriod=str(2 * 24 * 3600)  # Retain messages for 2 days
    )
)
def launch_from_notification_queue(event, context):
    for event_record in event['Records']:
        dispatch(event_record['body'].split())
