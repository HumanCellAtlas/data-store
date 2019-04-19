import os

from dss.config import Replica
from dss.util.aws.clients import dynamodb  # type: ignore
from dss.util.dynamodb import DynamoOwnershipLookup


class CollectionData:
    REPLICA = 'replica'
    OWNER = 'owner'
    UUID = 'uuid'
    VERSION = 'version'


class CollectionLookup(DynamoOwnershipLookup):
    def __init__(self):
        self.db_table_template = f"dss-collections-db-{{}}-{os.environ['DSS_DEPLOYMENT_STAGE']}"

    def put_collection(self, doc: dict):
        """Adds a new owner associated collection to the table if it does not already exist."""
        dynamodb.put_item(
            TableName=self.db_table_template.format(doc[CollectionData.REPLICA]),
            Item={
                'hash_key': {
                    'S': doc[CollectionData.OWNER]
                },
                'sort_key': {
                    'S': f'{doc[CollectionData.UUID]}.{doc[CollectionData.VERSION]}'
                },
                'body': {
                    'S': 'owner'  # comma-delimited string
                }
            }
        )

    def get_collection(self, replica: Replica, owner: str, uuid: str):
        """Returns all versions of a collection UUID for a user."""
        db_resp = dynamodb.get_item(
            TableName=self.db_table_template.format(replica.name),
            Key={
                'hash_key': {
                    'S': owner
                },
                'sort_key': {
                    'S': uuid
                }
            }
        )
        items = [db_resp.get('Item', None)] if db_resp.get('Item', None) else []
        return self.collections_from_items(items)

    def collections_from_items(self, items: list) -> list:
        collections = []
        for item in items:
            for uuid, version in item['sort_key']['S'].split('.'):
                if collections and collections[-1]['collection_uuid'] == uuid:

                collections.append({'collection_uuid': item['sort_key']['S'],
                                    'collection_versions': versions})
        return collections

    def get_collections_for_owner(self, replica: Replica, owner: str) -> list:
        db_resp = dynamodb.query(
            TableName=self.db_table_template.format(replica.name),
            KeyConditionExpression="hash_key=:owner",
            ExpressionAttributeValues={':owner': {'S': owner}}
        )
        return self.collections_from_items(db_resp.get('Items', []))

    def delete_collection(self, replica: Replica, owner: str, uuid: str):
        dynamodb.delete_item(
            TableName=self.db_table_template.format(replica.name),
            Key={
                'hash_key': {
                    'S': owner
                },
                'sort_key': {
                    'S': uuid
                }
            }
        )
