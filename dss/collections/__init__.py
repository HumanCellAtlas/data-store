import os

from dss.config import Replica
from dss.util.aws.clients import dynamodb  # type: ignore


_collectionsdb_table_template = f"dss-collections-db-{{}}-{os.environ['DSS_DEPLOYMENT_STAGE']}"


class CollectionData:
    REPLICA = 'replica'
    OWNER = 'owner'
    UUID = 'uuid'
    VERSION = 'version'


def put_collection(doc: dict):
    """Edits an existing item's attributes, or adds a new item to the table if it does not already exist."""
    curr = get_collection(Replica[doc[CollectionData.REPLICA]], doc[CollectionData.OWNER], doc[CollectionData.UUID])
    ver = f'{doc[CollectionData.VERSION]},{curr[0]["collection_versions"]}' if curr else doc[CollectionData.VERSION]
    dynamodb.update_item(
        TableName=_collectionsdb_table_template.format(doc[CollectionData.REPLICA]),
        Item={
            'hash_key': {
                'S': doc[CollectionData.OWNER]
            },
            'sort_key': {
                'S': doc[CollectionData.UUID]
            },
            'versions': {
                'S': ver  # comma-delimited string
            }
        }
    )


def get_collection(replica: Replica, owner: str, uuid: str):
    """Returns all versions of a collection UUID for a user."""
    db_resp = dynamodb.get_item(
        TableName=_collectionsdb_table_template.format(replica.name),
        Key={
            'hash_key': {
                'S': owner
            },
            'sort_key': {
                'S': uuid
            }
        }
    )
    return collections_from_items(db_resp.get('Item', []))


def collections_from_items(items: list) -> list:
    collections = []
    for item in items:
        versions = [i.strip() for i in item['versions']['S'].split(',') if i.strip()]
        collections.append({'collection_uuid': item['sort_key']['S'],
                            'collection_versions': versions})
    return collections


def get_collections_for_owner(replica: Replica, owner: str) -> list:
    db_resp = dynamodb.query(
        TableName=_collectionsdb_table_template.format(replica.name),
        KeyConditionExpression="hash_key=:owner",
        ExpressionAttributeValues={':owner': {'S': owner}}
    )
    return collections_from_items(db_resp['Items'])


def get_collections_for_replica(replica: Replica) -> list:
    db_resp = dynamodb.scan(TableName=_collectionsdb_table_template.format(replica.name))
    return collections_from_items(db_resp['Items'])


def delete_collection(replica: Replica, owner: str, uuid: str):
    dynamodb.delete_item(
        TableName=_collectionsdb_table_template.format(replica.name),
        Key={
            'hash_key': {
                'S': owner
            },
            'sort_key': {
                'S': uuid
            }
        }
    )
