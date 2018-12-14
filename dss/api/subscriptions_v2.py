import os
import datetime
import logging
from uuid import uuid4

import requests
from flask import jsonify, request
import jmespath
from jmespath.exceptions import JMESPathError

from dss.config import Replica
from dss.error import DSSException
from dss.util import security
from dss.subscriptions_v2 import (SubscriptionData, get_subscription, get_subscriptions_for_owner, put_subscription,
                                  delete_subscription)


logger = logging.getLogger(__name__)


@security.authorized_group_required(['hca', 'public'])
def get(uuid: str, replica: str):
    owner = security.get_token_email(request.token_info)
    subscription = get_subscription(Replica[replica], owner, uuid)
    if subscription is None or owner != subscription[SubscriptionData.OWNER]:
        raise DSSException(404, "not_found", "Cannot find subscription!")
    return subscription, requests.codes.ok


@security.authorized_group_required(['hca', 'public'])
def find(replica: str):
    owner = security.get_token_email(request.token_info)
    subs = [subscription for subscription in get_subscriptions_for_owner(Replica[replica], owner)
            if owner == subscription['owner']]
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
                requests.codes.unprocessable,
                "invalid_jmespath",
                "JMESPath query is invalid"
            )
    # TODO: check that attachment JMESPath filters will parse? - Brian Hnnafious 2019.01.25

    put_subscription(subscription_doc)
    return subscription_doc, requests.codes.created


@security.authorized_group_required(['hca', 'public'])
def delete(uuid: str, replica: str):
    owner = security.get_token_email(request.token_info)
    subscription = get_subscription(Replica[replica], owner, uuid)
    if subscription is None or owner != subscription[SubscriptionData.OWNER]:
        raise DSSException(404, "not_found", "Cannot find subscription!")
    delete_subscription(Replica[replica], owner, uuid)
    timestamp = datetime.datetime.utcnow()
    time_deleted = timestamp.strftime("%Y-%m-%dT%H%M%S.%fZ")
    return jsonify({'timeDeleted': time_deleted}), requests.codes.okay
