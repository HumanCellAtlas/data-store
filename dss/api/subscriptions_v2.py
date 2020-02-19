import datetime
import jmespath
import requests
from uuid import uuid4
from flask import jsonify, request
from jmespath.exceptions import JMESPathError

from dss.config import Replica
from dss.error import DSSException
from dss.util import security
from dss.util.auth import helpers
from dss.config import SUBSCRIPTION_LIMIT
from dss.subscriptions_v2 import (SubscriptionData,
                                  get_subscription,
                                  put_subscription,
                                  delete_subscription,
                                  get_subscriptions_for_owner,
                                  count_subscriptions_for_owner)


@security.assert_security(['hca', 'public'])
def get(uuid: str, replica: str):
    owner = helpers.get_token_email(request.token_info)
    subscription = get_subscription(Replica[replica], owner, uuid)
    if subscription is None or owner != subscription[SubscriptionData.OWNER]:
        raise DSSException(404, "not_found", "Cannot find subscription!")
    if 'hmac_secret_key' in subscription:
        subscription.pop('hmac_secret_key')
    return subscription, requests.codes.ok


@security.assert_security(['hca', 'public'])
def find(replica: str):
    owner = helpers.get_token_email(request.token_info)
    subscriptions = get_subscriptions_for_owner(Replica[replica], owner)
    for subscription in subscriptions:
        subscription['replica'] = Replica[replica].name
        if 'hmac_secret_key' in subscription:
            subscription.pop('hmac_secret_key')
    return {'subscriptions': subscriptions}, requests.codes.ok


@security.assert_security(['hca', 'public'])
def put(json_request_body: dict, replica: str):
    owner = helpers.get_token_email(request.token_info)
    if count_subscriptions_for_owner(Replica[replica], owner) > SUBSCRIPTION_LIMIT:
        raise DSSException(requests.codes.not_acceptable, "not_acceptable",
                           f"Users cannot exceed {SUBSCRIPTION_LIMIT} subscriptions!")

    subscription_doc = json_request_body.copy()
    subscription_doc[SubscriptionData.OWNER] = helpers.get_token_email(request.token_info)
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
    put_subscription(subscription_doc)
    return subscription_doc, requests.codes.created


@security.assert_security(['hca', 'public'])
def delete(uuid: str, replica: str):
    owner = helpers.get_token_email(request.token_info)
    subscription = get_subscription(Replica[replica], owner, uuid)
    if subscription is None or owner != subscription[SubscriptionData.OWNER]:
        raise DSSException(404, "not_found", "Cannot find subscription!")
    delete_subscription(Replica[replica], owner, uuid)
    timestamp = datetime.datetime.utcnow()
    time_deleted = timestamp.strftime("%Y-%m-%dT%H%M%S.%fZ")
    return jsonify({'timeDeleted': time_deleted}), requests.codes.okay
