"""
This Lambda executes commands forwarded by the DSS operations CLI. Commands are forward via SQS by embedding the
command as plain text directly in the message body.

For example the command
```
scripts/dss-ops.py storage verify-referential integrity --replica aws --keys key1 key2 key
```
would be forwarded with the message
```
"storage verify-referential integrity --replica aws --keys key1 key2 key"
```
"""

import os
import sys
import logging

import domovoi

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
import dss.operations.storage
from dss.operations import dispatch

logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger(__name__)  # noqa
logger.setLevel(logging.WARNING)  # noqa
# TODO: Can log level be passed in through command arguments?

dss.Config.set_config(dss.BucketConfig.NORMAL)

app = domovoi.Domovoi()
app.log.setLevel(logging.WARNING)  # suppress domovoi info logs

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
