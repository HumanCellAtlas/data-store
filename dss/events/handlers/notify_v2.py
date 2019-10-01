import os
import re
import json
import requests
import urllib3
from requests_http_signature import HTTPSignatureAuth
import logging
import datetime
from uuid import uuid4

from functools import lru_cache
import jmespath
from jmespath.exceptions import JMESPathError
from dcplib.aws.sqs import SQSMessenger, get_queue_url

from dss import Config, Replica, datetime_to_version_format
from dss.subscriptions_v2 import SubscriptionData, update_subcription_stats
from dss.storage.identifiers import UUID_PATTERN, VERSION_PATTERN, TOMBSTONE_SUFFIX, DSS_BUNDLE_KEY_REGEX

logger = logging.getLogger(__name__)

notification_queue_name = "dss-notify-v2-" + os.environ['DSS_DEPLOYMENT_STAGE']
_attachment_size_limit = 128 * 1024

_versioned_tombstone_key_regex = re.compile(f"^(bundles)/({UUID_PATTERN}).({VERSION_PATTERN}).{TOMBSTONE_SUFFIX}$")
_unversioned_tombstone_key_regex = re.compile(f"^(bundles)/({UUID_PATTERN}).{TOMBSTONE_SUFFIX}$")
_bundle_key_regex = DSS_BUNDLE_KEY_REGEX


def should_notify(replica: Replica, subscription: dict, metadata_document: dict, key: str) -> bool:
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


def notify_or_queue(replica: Replica, subscription: dict, metadata_document: dict, key: str):
    """
    Notify or queue for later processing:
        1) For normal bundle: attempt notification, queue on failure
        2) For delete: attempt notification, queue on failure
        3) For versioned tombstone: attempt notification, queue on failure
        4) For unversioned tombstone: Queue one notification per affected bundle version. Notifications are
           not attempted for previously tombstoned versions. Since the number of versions is
           unbounded, inline delivery is not attempted.
    """
    event_type = metadata_document['event_type']
    with SQSMessenger(get_queue_url(notification_queue_name)) as sqsm:
        if _unversioned_tombstone_key_regex.match(key):
            tombstones = set()
            bundles = set()
            key_prefix = key.rsplit(".", 1)[0]  # chop off the tombstone suffix
            for key in _list_prefix(replica, key_prefix):
                if _versioned_tombstone_key_regex.match(key):
                    bundle_key = key.rsplit(".", 1)[0]
                    tombstones.add(bundle_key)
                elif _bundle_key_regex.match(key):
                    bundles.add(key)
            for key in bundles - tombstones:
                sqsm.send(_format_sqs_message(replica, subscription, event_type, key), delay_seconds=0)
        else:
            notify_status = notify(subscription, metadata_document, key)
            if notify_status:
                update_subcription_stats(subscription, True)
            else:
                sqsm.send(_format_sqs_message(replica, subscription, event_type, key), delay_seconds=15 * 60)
                update_subcription_stats(subscription, False)


def notify(subscription: dict, metadata_document: dict, key: str) -> bool:
    """
    Attempt notification delivery. Return True for success, False for failure
    """
    fqid = key.split("/")[1]
    bundle_uuid, bundle_version = fqid.split(".", 1)
    sfx = f".{TOMBSTONE_SUFFIX}"
    if bundle_version.endswith(sfx):
        bundle_version = bundle_version[:-len(sfx)]
    api_domain_name = f'https://{os.environ.get("API_DOMAIN_NAME")}'
    payload = {
        'bundle_url': api_domain_name + f'/v1/bundles/{bundle_uuid}?version={bundle_version}',
        'dss_api': api_domain_name,
        'subscription_id': subscription[SubscriptionData.UUID],
        'event_timestamp': datetime_to_version_format(datetime.datetime.utcnow()),
        'event_type': metadata_document['event_type'],
        'match': {
            'bundle_uuid': bundle_uuid,
            'bundle_version': bundle_version,
        },
        'transaction_id': str(uuid4())
    }

    jmespath_query = subscription.get(SubscriptionData.JMESPATH_QUERY)
    if jmespath_query is not None:
        payload[SubscriptionData.JMESPATH_QUERY] = jmespath_query

    if "CREATE" == metadata_document['event_type']:
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

def _format_sqs_message(replica: Replica, subscription: dict, event_type: str, key: str):
    return json.dumps({
        SubscriptionData.REPLICA: replica.name,
        SubscriptionData.OWNER: subscription['owner'],
        SubscriptionData.UUID: subscription['uuid'],
        'event_type': event_type,
        'key': key
    })

@lru_cache()
def _list_prefix(replica: Replica, prefix: str):
    handle = Config.get_blobstore_handle(replica)
    return [object_key for object_key in handle.list(replica.bucket, prefix)]
