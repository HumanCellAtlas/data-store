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
_attachment_size_limit = 128 * 1024

def should_notify(replica: Replica, subscription: dict, metadata_document: dict, event_type: str, key: str) -> bool:
    """
    Check if a notification should be attempted for subscription and key
    """
    jmespath_query = subscription.get(SubscriptionData.JMESPATH_QUERY)

    if not jmespath_query:
        return True
    else:
        try:
            if jmespath.search(jmespath_query, metadata_document):
                return True
            else:
                return False
        except JMESPathError:
            logger.error("jmespath query failed for owner={} replica={} uuid={} jmespath_query='{}' key={}".format(
                subscription[SubscriptionData.OWNER],
                subscription[SubscriptionData.REPLICA],
                subscription[SubscriptionData.UUID],
                subscription[SubscriptionData.JMESPATH_QUERY],
                key
            ))
            return False

def notify_or_queue(replica: Replica, subscription: dict, metadata_document: dict, event_type: str, key: str):
    """
    Notify or queue for later processing. There are three cases:
        1) For a normal bundle: attempt notification, queue on delivery failure
        2) For a versioned tombstone: attempt notifcation, queue on delivery failure
        3) Unversioned tombstone notify on an unbounded number of bundles. Send these directly to queue.
    """
    parts = key.split(".")
    if key.endswith("dead") and 2 == len(parts):  # This tests for unversioned tombstone key: bundles/{uuid}.dead
        tombstones = set()
        bundles = set()
        for bundle_key in _list_prefix(replica, parts[0]):
            if 2 < len(bundle_key.split(".")):
                if bundle_key.endswith(".dead"):
                    tombstones.add(bundle_key[:-5])
                else:
                    bundles.add(bundle_key)
        for bundle_key in bundles:
            if not bundle_key in tombstones:
                queue_notification(replica, subscription, event_type, bundle_key, delay_seconds=0)
    else:
        if not notify(subscription, metadata_document, event_type, key):
            queue_notification(replica, subscription, event_type, key)

def notify(subscription: dict, metadata_document: dict, event_type: str, key: str):
    """
    Attempt notification delivery. Return True for success, False for failure
    """
    fqid = key.split("/")[1]
    bundle_uuid, bundle_version = fqid.split(".", 1)

    payload = {
        'transaction_id': str(uuid4()),
        'subscription_id': subscription[SubscriptionData.UUID],
        'match': {
            'bundle_uuid': bundle_uuid,
            'bundle_version': bundle_version,
        }
    }

    jmespath_query = subscription.get(SubscriptionData.JMESPATH_QUERY)
    if jmespath_query is not None:
        payload[SubscriptionData.JMESPATH_QUERY] = jmespath_query

    attachments_defs = subscription.get(SubscriptionData.ATTACHMENTS)
    if attachments_defs is not None:
        errors = dict()
        attachments = dict()
        for name, attachment in attachments_defs.items():
            if 'jmespath' == attachment['type']:
                try:
                    value = jmespath.search(attachment['expression'], metadata_document)
                except BaseException as e:
                    errors[name] = str(e)
                else:
                    attachments[name] = value
        if errors:
            attachments['_errors'] = errors
        size = len(json.dumps(attachments).encode('utf-8'))
        if size > _attachment_size_limit:
            attachments = {'_errors': f"Attachments too large ({size} > {_attachment_size_limit})"}
        payload['attachments'] = attachments

    request = {
        'method': subscription.get(SubscriptionData.METHOD, "POST"),
        'url': subscription[SubscriptionData.CALLBACK_URL],
        'headers': dict(),
        'allow_redirects': False,
        'timeout': None,
    }

    hmac_key = subscription.get('hmac_secret_key')
    if hmac_key:
        hmac_key_id = subscription.get('hmac_key_id', "hca-dss:" + subscription['uuid'])
        request['auth'] = HTTPSignatureAuth(key=hmac_key.encode(), key_id=hmac_key_id)
        # get rid of this so it doesn't appear in delivery log messages
        del subscription['hmac_secret_key']
    else:
        request['auth'] = None

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
        logger.warning("Exception raised while delivering notification: %s, subscription: %s",
                       str(payload), str(subscription), exc_info=e)
        return False

    if 200 <= response.status_code < 300:
        logger.info("Successfully delivered %s: HTTP status %i, subscription: %s",
                    str(payload), response.status_code, str(subscription))
        return True
    else:
        logger.warning("Failed delivering %s: HTTP status %i, subscription: %s",
                       str(payload), response.status_code, str(subscription))
        return False


@lru_cache()
def build_bundle_metadata_document(replica: Replica, key: str) -> dict:
    """
    This returns a JSON document with bundle manifest and metadata files suitable for JMESPath filters.
    """
    handle = Config.get_blobstore_handle(replica)
    if key.endswith("dead"):
        manifest = json.loads(handle.get(replica.bucket, key).decode("utf-8"))
        return manifest
    else:
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

@lru_cache()
def _list_prefix(replica: Replica, prefix: str):
    handle = Config.get_blobstore_handle(replica)
    return [object_key for object_key in handle.list(replica.bucket, prefix)]

@lru_cache()
def _get_notification_queue_url():
    return sqs.get_queue_url(QueueName=notification_queue_name)['QueueUrl']
