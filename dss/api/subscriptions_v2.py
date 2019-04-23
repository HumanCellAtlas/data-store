import datetime
import json
from uuid import uuid4

import requests
from flask import jsonify, request
import jmespath
from jmespath.exceptions import JMESPathError

from dss.config import Replica
from dss.error import DSSException
from dss.util import security, dynamodb


subscription_db_table = f"dss-subscriptions-v2-{{}}-{os.environ['DSS_DEPLOYMENT_STAGE']}"


class SubscriptionData:
    REPLICA = 'replica'
    OWNER = 'owner'
    UUID = 'uuid'
    CALLBACK_URL = 'callback_url'
    JMESPATH_QUERY = 'jmespath_query'
    METHOD = 'method'
    ENCODING = 'encoding'
    FORM_FIELDS = 'form_fields'
    PAYLOAD_FORM_FIELD = 'payload_form_field'
    ATTACHMENTS = 'attachments'


@security.authorized_group_required(['hca', 'public'])
def get(uuid: str, replica: str):
    owner = security.get_token_email(request.token_info)
    subscription = dynamodb.get_item(table=subscription_db_table.format(Replica[replica].name),
                                     key1=owner,
                                     key2=uuid)
    if subscription is not None:
        subscription = json.loads(subscription['body']['S'])
    if subscription is None or owner != subscription[SubscriptionData.OWNER]:
        raise DSSException(404, "not_found", "Cannot find subscription!")
    return subscription, requests.codes.ok


@security.authorized_group_required(['hca', 'public'])
def find(replica: str):
    owner = security.get_token_email(request.token_info)
    subscriptions = dynamodb.get_primary_key_items(table=subscription_db_table.format(Replica[replica].name),
                                                   key=owner)
    subs = [json.loads(s['body']['S']) for s in subscriptions if owner == s['body']['S']['owner']]
    for s in subs:
        s['replica'] = Replica[replica].name
    return {'subscriptions': subs}, requests.codes.ok


@security.authorized_group_required(['hca', 'public'])
def put(json_request_body: dict, replica: str):
    subscription_doc = json_request_body.copy()
    subscription_doc[SubscriptionData.OWNER] = security.get_token_email(request.token_info)
    subscription_uuid = str(uuid4())
    subscription_doc[SubscriptionData.UUID] = subscription_uuid
    subscription_doc[SubscriptionData.REPLICA] = Replica[replica].name
    if subscription_doc.get(SubscriptionData.JMESPATH_QUERY) is not None:
        try:
            jmespath.compile(subscription_doc[SubscriptionData.JMESPATH_QUERY])
        except JMESPathError:
            raise DSSException(
                requests.codes.bad_request,
                "invalid_jmespath",
                "JMESPath query is invalid"
            )
    # validate attachment JMESPath if present
    attachments = subscription_doc.get(SubscriptionData.ATTACHMENTS)
    if attachments is not None:
        for name, definition in attachments.items():
            if name.startswith('_'):
                raise DSSException(requests.codes.bad_request,
                                   "invalid_attachment_name",
                                   f"Attachment names must not start with underscore ({name})")
            type_ = definition['type']
            if type_ == 'jmespath':
                expression = definition['expression']
                try:
                    jmespath.compile(expression)
                except JMESPathError as e:
                    raise DSSException(requests.codes.bad_request,
                                       "invalid_attachment_expression",
                                       f"Unable to compile JMESPath expression for attachment {name}") from e
            else:
                assert False, type_
        dynamodb.put_item(table=subscription_db_table.format(Replica[replica].name),
                          key1=subscription_doc[SubscriptionData.OWNER],
                          key2=subscription_doc[SubscriptionData.UUID],
                          value=json.dumps(subscription_doc))
    return subscription_doc, requests.codes.created


@security.authorized_group_required(['hca', 'public'])
def delete(uuid: str, replica: str):
    owner = security.get_token_email(request.token_info)
    subscription = dynamodb.get_item(table=subscription_db_table.format(Replica[replica].name),
                                     key1=owner,
                                     key2=uuid)
    if subscription is not None:
        subscription = json.loads(subscription['body']['S'])
    if subscription is None or owner != subscription[SubscriptionData.OWNER]:
        raise DSSException(404, "not_found", "Cannot find subscription!")
        # subscription_lookup.delete_subscription(Replica[replica], owner, uuid)
    timestamp = datetime.datetime.utcnow()
    time_deleted = timestamp.strftime("%Y-%m-%dT%H%M%S.%fZ")
    return jsonify({'timeDeleted': time_deleted}), requests.codes.okay
