import os
import sys
import json
import logging
from urllib.parse import unquote

import boto3
import domovoi

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib')) # noqa
sys.path.insert(0, pkg_root) # noqa

from dss.events.handlers.index import process_new_indexable_object

app = domovoi.Domovoi()

s3_bucket = os.environ.get("DSS_S3_TEST_BUCKET")

@app.s3_event_handler(bucket=s3_bucket, events=["s3:ObjectCreated:*"])
def dispatch_indexer_event(event, context) -> None:
    app.log.setLevel(logging.DEBUG)
    process_new_indexable_object(event, logger=app.log)
