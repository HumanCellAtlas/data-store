import json
import logging
import os
import sys

import domovoi

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss.events.handlers.index import process_new_s3_indexable_object, process_new_gs_indexable_object
from dss.events.handlers.index import process_s3_removed_object

app = domovoi.Domovoi()

dss.Config.set_config(dss.DeploymentStage.NORMAL)

s3_bucket = dss.Config.get_s3_bucket()

@app.s3_event_handler(bucket=s3_bucket, events=["s3:ObjectCreated:*", "s3:ObjectRemoved:Delete"])
def dispatch_s3_indexer_event(event, context) -> None:
    app.log.setLevel(logging.DEBUG)
    if event.get("Event") == "s3:TestEvent":
        app.log.info("DSS index daemon received S3 test event")
    else:
        if "ObjectRemoved" in event["Records"][0]["eventName"]:
            process_s3_removed_object(event, app.log)
        else:
            process_new_s3_indexable_object(event, app.log)

@app.sns_topic_subscriber("dss-gs-bucket-events-" + os.environ["DSS_GS_BUCKET"])
def dispatch_gs_indexer_event(event, context):
    """
    This handler receives GS events via the Google Cloud Function deployed from daemons/dss-gs-event-relay.
    """
    gs_event = json.loads(event['Records'][0]['Sns']['Message'])
    process_new_gs_indexable_object(gs_event['data'], app.log)
