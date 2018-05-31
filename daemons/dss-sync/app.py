"""
See Readme.md in this directory for documentation on the dss-sync daemon.
"""

import os
import sys
import logging
import json
from urllib.parse import unquote

import domovoi

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss.logging import configure_lambda_logging
from dss.events.handlers.sync import sync_blob, sns_topics, copy_parts_handler, complete_multipart_upload, \
    compose_upload
from dss.util import tracing
from dss.util.aws import resources
from dss.config import Replica


logger = logging.getLogger(__name__)

configure_lambda_logging()
app = domovoi.Domovoi(configure_logs=False)

dss.Config.set_config(dss.BucketConfig.NORMAL)

s3_bucket = dss.Config.get_s3_bucket()


@app.s3_event_handler(bucket=s3_bucket, events=["s3:ObjectCreated:*"])
def process_new_s3_syncable_object(event, context):
    if event.get("Event") == "s3:TestEvent":
        app.log.info("DSS sync daemon received S3 test event")
    else:
        bucket = resources.s3.Bucket(event['Records'][0]["s3"]["bucket"]["name"])
        obj = bucket.Object(unquote(event['Records'][0]["s3"]["object"]["key"]))
        sync_blob(source_platform="s3", source_key=obj.key, dest_platform="gs", context=context)

@app.sns_topic_subscriber("dss-gs-bucket-events-" + os.environ["DSS_GS_BUCKET"])
def process_new_gs_syncable_object(event, context):
    """
    This handler receives GS events via SNS through the Google Cloud Function deployed from daemons/dss-gs-event-relay.
    """
    gs_event = json.loads(event["Records"][0]["Sns"]["Message"])
    gs_key_name = gs_event["data"]["name"]
    sync_blob(source_platform="gs", source_key=gs_key_name, dest_platform="s3", context=context)

@app.sns_topic_subscriber(sns_topics["closer"]["gs"])
def closer_gs(event, context):
    msg = json.loads(event["Records"][0]["Sns"]["Message"])
    compose_upload(msg)

@app.sns_topic_subscriber(sns_topics["closer"]["s3"])
def closer_s3(event, context):
    msg = json.loads(event["Records"][0]["Sns"]["Message"])
    complete_multipart_upload(msg)

platform_to_replica = dict(s3=Replica.aws, gs=Replica.gcp)
@app.sns_topic_subscriber(sns_topics["copy_parts"])
def copy_parts(event, context):

    topic_arn = event["Records"][0]["Sns"]["TopicArn"]
    msg = json.loads(event["Records"][0]["Sns"]["Message"])
    copy_parts_handler(topic_arn, msg, platform_to_replica)
