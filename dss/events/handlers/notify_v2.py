import os
import json
import requests
import urllib3
from requests_http_signature import HTTPSignatureAuth
import logging
from uuid import uuid4
from collections import defaultdict

from functools import lru_cache
import jmespath
from jmespath.exceptions import JMESPathError
import boto3

import dss
from dss import Config, Replica
from dss.util.aws.clients import dynamodb, sqs  # type: ignore
from dss.subscriptions_v2 import SubscriptionData, get_subscriptions_for_replica

logger = logging.getLogger(__name__)

notification_queue_name = "dss-notify-v2-" + os.environ['DSS_DEPLOYMENT_STAGE']
subscription_ddb_table = "bhannafi-test-subscriptions-" + os.environ['DSS_DEPLOYMENT_STAGE']

class DSSNotificationDeliveryFailure(Exception):
    pass

def should_notify(replica: Replica, subscription: dict, event_type: str, key: str) -> bool:
    """
    Check if a notification should be attempted for subscription and key
    """
    jmespath_query = subscription.get(SubscriptionData.JMESPATH_QUERY)

    if not jmespath_query:
        print("XXX" "No JMESPath", str(subscription))
        return True
    else:
        replica = Replica[subscription[SubscriptionData.REPLICA]]
        doc = build_metadata_document(replica, key)
        try:
            if jmespath.search(jmespath_query, doc):
                print("XXX" "doc passes JMESPath", str(subscription))
                return True
            else:
                print("XXX" "doc does not pass JMESPath", str(subscription))
                return False
        except JMESPathError:
            logger.error("jmespath query failed for owner={} replica={} uuid={} jmespath_query='{}' key={}".format(
                subscription[SubscriptionData.OWNER],
                subscription[SubscriptionData.REPLICA],
                subscription[SubscriptionData.UUID],
                subscription[SubscriptionData.JMESPATH_QUERY],
                key
            ))
            print("XXX" "JMESPath error", str(subscription))
            return False

def notify_or_queue(replica: Replica, subscription: dict, event_type: str, key: str):
    try:
        notify(subscription, event_type, key)
    except DSSNotificationDeliveryFailure:
        queue_notification(replica, subscription, event_type, key)

def notify(subscription: dict, event_type: str, key: str):
    fqid = key.split("/")[1]
    bundle_uuid, bundle_version = fqid.split(".", 1)

    payload = {
        'transaction_id': str(uuid4()),
        'subscription_id': subscription['uuid'],
        'match': {
            'bundle_uuid': bundle_uuid,
            'bundle_version': bundle_version,
        }
    }
    jmespath_query = subscription.get(SubscriptionData.JMESPATH_QUERY)
    if jmespath_query is not None:
        payload[SubscriptionData.JMESPATH_QUERY] = jmespath_query

#    definitions = subscription.get('attachments')
#    if definitions is not None:
#        payload['attachments'] = attachment.select(definitions, doc)

    request = {
        'method': subscription.get(SubscriptionData.METHOD, "POST"),
        'url': subscription[SubscriptionData.CALLBACK_URL],
        'headers': dict(),
        'allow_redirects': False,
        'timeout': None,
    }

    encoding = subscription.get(SubscriptionData.ENCODING, "application/json")
    if encoding == "application/json":
        request['json'] = payload
    elif encoding == 'multipart/form-data':
        body = subscription[SubscriptionData.FORM_FIELDS].copy()
        body[subscription[SubscriptionData.PAYLOAD_FORM_FIELD]] = json.dumps(payload)
        data, content_type = urllib3.encode_multipart_formdata(body)
        request['data'] = data
        request['headers']['Content-Type'] = content_type
    else:
        raise ValueError(f"Encoding {encoding} is not supported")

    try:
        response = requests.request(**request)
    except BaseException as e:
        logger.warning("Exception raised while delivering %s:", exc_info=e)
        return False

    if 200 <= response.status_code < 300:
        logger.info("Successfully delivered %s: HTTP status %i, subscription: %s",
                    # TODO: better log response
                    str(payload), response.status_code, str(subscription))
        return True
    else:
        print("FAILED", response.content)
        logger.warning("Failed delivering %s: HTTP status %i, subscription: %s",
                       # TODO: better log response
                       str(payload), response.status_code, str(subscription))
        return False


@lru_cache()
def build_metadata_document(replica: Replica, key: str) -> dict:
    """
    This returns a JSON document with bundle manifest and metadata files suitable for JMESPath filters.
    """
    handle = Config.get_blobstore_handle(replica)
    manifest = json.loads(handle.get(replica.bucket, key).decode("utf-8"))
    files: dict = defaultdict(list)
    for file in manifest['files']:
        if "application/json" == file['content-type']:
            blob_key = "blobs/{}.{}.{}.{}".format(
                file['sha256'],
                file['sha1'],
                file['s3-etag'],
                file['crc32c'],
            )
            contents = handle.get(replica.bucket, blob_key).decode("utf-8")
            try:
                files[file['name']].append(json.loads(contents))
            except json.decoder.JSONDecodeError:
                logging.info(f"{file['name']} not json decodable")

    return {
        'manifest': manifest,
        'files': dict(files),
    }

def queue_notification(replica: Replica, subscription: dict, event_type: str, key: str, delay_seconds=15 * 60):
    sqs.send_message(
        QueueUrl=_get_notification_queue_url(),
        MessageBody=json.dumps({
            SubscriptionData.REPLICA: replica.name,
            SubscriptionData.OWNER: subscription['owner'],
            SubscriptionData.UUID: subscription['uuid'],
            'event_type': event_type,
            'key': key
        }),
        DelaySeconds=delay_seconds
    )

def queue_notifications(replica: Replica, key: str, subscription_uuid: str):
    # TODO: queue messages in batches? -  BrianH
    for subscription in get_subscriptions_for_replica(replica):
        sqs.send_message(
            QueueUrl=_get_notification_queue_url(),
            MessageBody=json.dumps({
                "replica": replica.name,
                "object_key": key,
                "subscription": {
                    "owner": subscription[SubscriptionData.OWNER],
                    "uuid": subscription[SubscriptionData.UUID],
                }
            }),
        )

@lru_cache()
def _get_notification_queue_url():
    return sqs.get_queue_url(QueueName=notification_queue_name)['QueueUrl']
