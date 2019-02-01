"""
See Readme.md in this directory for documentation on the dss-notify-v2 daemon.

storage_event -> invoke_notify_daemon -> invoke_sfn -> sfn_dynamodb_loop -> sqs -> invoke_notify_daemon
"""

import os, sys, json, logging
import boto3
from urllib.parse import unquote
from concurrent.futures import ThreadPoolExecutor

import domovoi

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss import Config, Replica
from dss.logging import configure_lambda_logging
from dss.subscriptions_v2 import get_subscriptions_for_replica, get_subscription
from dss.events.handlers.notify_v2 import should_notify, notify_or_queue, build_bundle_metadata_document

configure_lambda_logging()
logger = logging.getLogger(__name__)
dss.Config.set_config(dss.BucketConfig.NORMAL)

app = domovoi.Domovoi()

# This entry point is for S3 native events forwarded through SQS.
@app.s3_event_handler(bucket=Config.get_s3_bucket(), events=["s3:ObjectCreated:*"], use_sqs=True)
def launch_from_s3_event(event, context):
    replica = Replica.aws
    if event.get("Event") == "s3:TestEvent":
        logger.info("S3 test event received and processed successfully")
    else:
        for event_record in event['Records']:
            bucket = event_record['s3']['bucket']['name']
            if bucket != replica.bucket:
                logger.error("Received S3 event for bucket %s with no configured replica", bucket)
                continue
            key = unquote(event_record['s3']['object']['key'])
            # TODO:
            # Parallelize sending subscriptions
            # - xbrianh
            if key.startswith("bundles"):
                for subscription in get_subscriptions_for_replica(replica):
                    metadata_document = build_bundle_metadata_document(replica, key)
                    if should_notify(replica, subscription, metadata_document, "CREATE", key):
                        notify_or_queue(replica, subscription, metadata_document, "CREATE", key)
            else:
                logger.warning(f"Notifications not supported for {key}")

# This entry point is for external events forwarded by dss-gs-event-relay (or other event sources) through SNS-SQS.
@app.sqs_queue_subscriber("dss-notify-v2-event-relay-" + os.environ['DSS_DEPLOYMENT_STAGE'])
def launch_from_forwarded_event(event, context):
    replica = Replica.gcp
    for event_record in event['Records']:
        message = json.loads(json.loads(event_record['body'])['Message'])
        if message['selfLink'].startswith("https://www.googleapis.com/storage"):
            key = message['name']
            # TODO:
            # Parallelize sending subscriptions
            # - xbrianh
            if key.startswith("bundles"):
                for subscription in get_subscriptions_for_replica(Replica.gcp):
                    metadata_document = build_bundle_metadata_document(replica, key)
                    if should_notify(replica, subscription, metadata_document, "CREATE", key):
                        notify_or_queue(replica, subscription, metadata_document, "CREATE", key)
            else:
                logger.warning(f"Notifications not supported for {key}")
        else:
            raise NotImplementedError()

# This entry point is for queued notifications for manual notification or redrive
@app.sqs_queue_subscriber("dss-notify-v2-" + os.environ['DSS_DEPLOYMENT_STAGE'])
def launch_from_notification_queue(event, context):
    for event_record in event['Records']:
        message = json.loads(event_record['body'])
        replica = Replica[message['replica']]
        owner = message['owner']
        uuid = message['uuid']
        key = message['key']
        event_type = message['event_type']
        subscription = get_subscription(replica, owner, uuid)
        metadata_document = build_bundle_metadata_document(replica, key)
        if subscription is not None:
            notify_or_queue(replica, subscription, metadata_document, event_type, key)
        else:
            logger.warning(f"Recieved queue message with no matching subscription:{message}")
