import os
import json
from functools import lru_cache

from dss.config import Replica
from dss.util.aws.clients import dynamodb  # type: ignore


_ddb_table_template = f"dss-subscriptions-v2-{{}}-{os.environ['DSS_DEPLOYMENT_STAGE']}"


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


def put_subscription(doc: dict):
    dynamodb.put_item(
        TableName=_ddb_table_template.format(doc[SubscriptionData.REPLICA]),
        Item={
            'hash_key': {
                'S': doc[SubscriptionData.OWNER]
            },
            'sort_key': {
                'S': doc[SubscriptionData.UUID]
            },
            'body': {
                'S': json.dumps(doc)
            },
        }
    )

def get_subscription(replica: Replica, owner: str, uuid: str) -> dict:
    db_resp = dynamodb.get_item(
        TableName=_ddb_table_template.format(replica.name),
        Key={
            'hash_key': {
                'S': owner
            },
            'sort_key': {
                'S': uuid
            }
        }
    )
    item = db_resp.get('Item')
    if item is not None:
        return json.loads(item['body']['S'])
    else:
        return None

def get_subscriptions_for_owner(replica: Replica, owner: str) -> list:
    db_resp = dynamodb.query(
        TableName=_ddb_table_template.format(replica.name),
        KeyConditionExpression="#hash_key=:owner",
        ExpressionAttributeNames={'#hash_key': "hash_key"},
        ExpressionAttributeValues={':owner': {'S': owner}}
    )
    subscriptions = [json.loads(item['body']['S']) for item in db_resp['Items']]
    return subscriptions

def get_subscriptions_for_replica(replica: Replica) -> list:
    db_resp = dynamodb.scan(TableName=_ddb_table_template.format(replica.name))
    subscriptions = [json.loads(item['body']['S']) for item in db_resp['Items']]
    return subscriptions

def delete_subscription(replica: Replica, owner: str, uuid: str):
    dynamodb.delete_item(
        TableName=_ddb_table_template.format(replica.name),
        Key={
            'hash_key': {
                'S': owner
            },
            'sort_key': {
                'S': uuid
            }
        }
    )
