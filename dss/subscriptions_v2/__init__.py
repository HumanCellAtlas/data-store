import os
import json

from dss.config import Replica
from dss import dynamodb  # type: ignore


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
    BLANK_STATS = {"attempted": 0, "successful": 0, "failed": 0}


subscription_db_table = f"dss-subscriptions-v2-{{}}-{os.environ['DSS_DEPLOYMENT_STAGE']}"


def update_subcription_stats(doc: dict, status: bool):
    status = 'successful' if status else 'failed'
    item = json.loads(dynamodb.get_item(table=subscription_db_table.format(doc[SubscriptionData.REPLICA]),
                                        hash_key=doc[SubscriptionData.OWNER],
                                        sort_key=doc[SubscriptionData.UUID]))
    current_stats = item.get(SubscriptionData.STATS, SubscriptionData.BLANK_STATS)
    current_stats[status] += 1
    current_stats['attempted'] += 1
    item[SubscriptionData.STATS] = current_stats
    put_subscription(item)


def put_subscription(doc: dict):
    dynamodb.put_item(table=subscription_db_table.format(doc[SubscriptionData.REPLICA]),
                      hash_key=doc[SubscriptionData.OWNER],
                      sort_key=doc[SubscriptionData.UUID],
                      value=json.dumps(doc))


def get_subscription(replica: Replica, owner: str, uuid: str):
    try:
        item = dynamodb.get_item(table=subscription_db_table.format(replica.name),
                                 hash_key=owner,
                                 sort_key=uuid)
        return json.loads(item)
    except dynamodb.DynamoDBItemNotFound:
        return None


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
