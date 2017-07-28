import os
import sys
import logging

import domovoi

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss.events.handlers.index import process_new_indexable_object

app = domovoi.Domovoi()

dss.Config.set_config(dss.DeploymentStage.NORMAL)

s3_bucket = dss.Config.get_s3_bucket()

@app.s3_event_handler(bucket=s3_bucket, events=["s3:ObjectCreated:*"])
def dispatch_indexer_event(event, context) -> None:
    app.log.setLevel(logging.DEBUG)
    if event.get("Event") == "s3:TestEvent":
        app.log.info("DSS index daemon received S3 test event")
    else:
        process_new_indexable_object(event, logger=app.log)
