import os
import json
import typing

from dss.config import Replica
from dss import dynamodb  # type: ignore


class SubscriptionStats:
    ATTEMPTS = 'attempts'
    SUCCESSFUL = 'successful'
    FAILED = 'failed'


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
    STATS = 'stats'


subscription_db_table = f"dss-subscriptions-v2-{{}}-{os.environ['DSS_DEPLOYMENT_STAGE']}"


def update_subscription_stats(doc: dict, status: str):
    update_expression = f"ADD {SubscriptionStats.ATTEMPTS} :q, {status} :q"
    expression_attribute_value = {":q": {"N": "1"}}
    dynamodb.update_item(table=subscription_db_table.format(doc[SubscriptionData.REPLICA]),
                         hash_key=doc[SubscriptionData.OWNER],
                         sort_key=doc[SubscriptionData.UUID],
                         update_expression=update_expression,
                         expression_attribute_values=expression_attribute_value)


def put_subscription(doc: dict):
    dynamodb.put_item(table=subscription_db_table.format(doc[SubscriptionData.REPLICA]),
                      hash_key=doc[SubscriptionData.OWNER],
                      sort_key=doc[SubscriptionData.UUID],
                      value=json.dumps(doc))


def get_subscription(replica: Replica, owner: str, uuid: str):
    try:
        item = dynamodb.get_all_key_attributes(table=subscription_db_table.format(replica.name),
                                               hash_key=owner,
                                               sort_key=uuid)
    except dynamodb.DynamoDBItemNotFound:
        return None
    payload = json.loads(item['body'])
    stats = {}  # type: typing.Dict
    for attribute_type in SubscriptionStats:
        attribute_value = item.get(attribute_type, None)
        if attribute_value:
            stats[attribute_type] = attribute_value
    payload[SubscriptionData.STATS] = stats
    return payload


def get_subscriptions_for_owner(replica: Replica, owner: str) -> list:
    items = dynamodb.get_primary_key_items(table=subscription_db_table.format(replica.name),
                                           key=owner)
    return [json.loads(item) for item in items]


def count_subscriptions_for_owner(replica: Replica, owner: str) -> int:
    return dynamodb.get_primary_key_count(table=subscription_db_table.format(replica.name),
                                          key=owner)


def get_subscriptions_for_replica(replica: Replica) -> list:
    items = dynamodb.get_all_table_items(table=subscription_db_table.format(replica.name))
    return [json.loads(item) for item in items]


def delete_subscription(replica: Replica, owner: str, uuid: str):
    dynamodb.delete_item(table=subscription_db_table.format(replica.name),
                         hash_key=owner,
                         sort_key=uuid)
