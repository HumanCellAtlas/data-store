import os
import sys
import logging
from urllib.parse import unquote

import boto3
import domovoi

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss.events.handlers.sync import sync_blob

app = domovoi.Domovoi()

dss.Config.set_config(dss.BucketStage.NORMAL)

s3_bucket = dss.Config.get_s3_bucket()

@app.s3_event_handler(bucket=s3_bucket, events=["s3:ObjectCreated:*"])
def process_new_syncable_object(event, context):
    app.log.setLevel(logging.DEBUG)
    if event.get("Event") == "s3:TestEvent":
        app.log.info("DSS sync daemon received S3 test event")
    else:
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(event['Records'][0]["s3"]["bucket"]["name"])
        obj = bucket.Object(unquote(event['Records'][0]["s3"]["object"]["key"]))
        sync_blob(source_platform="s3", source_key=obj.key, dest_platform="gs", logger=app.log)
