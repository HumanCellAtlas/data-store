import json

from dss.config import Replica
from dss.util.aws.clients import dynamodb  # type: ignore


class DynamoOwnershipLookup(object):
    def __init__(self):
        self.db_table_template = 'base_template'

    def put_item(self, replica: Replica, owner: str, key: str, value: str):
        dynamodb.put_item(
            TableName=self.db_table_template.format(replica.name),
            Item={
                'hash_key': {
                    'S': owner
                },
                'sort_key': {
                    'S': key
                },
                'body': {
                    'S': value
                },
            }
        )

    def get_item(self, replica: Replica, owner: str, key: str) -> dict:
        db_resp = dynamodb.get_item(
            TableName=self.db_table_template.format(replica.name),
            Key={
                'hash_key': {
                    'S': owner
                },
                'sort_key': {
                    'S': key
                }
            }
        )
        item = db_resp.get('Item')
        if item is not None:
            return json.loads(item['body']['S'])
        else:
            return None

    def get_items_for_owner(self, replica: Replica, owner: str) -> list:
        db_resp = dynamodb.query(
            TableName=self.db_table_template.format(replica.name),
            ScanIndexForward=False,  # True = ascending, False = descending
            KeyConditionExpression="#hash_key=:owner",
            ExpressionAttributeNames={'#hash_key': "hash_key"},
            ExpressionAttributeValues={':owner': {'S': owner}}
        )
        subscriptions = [json.loads(item['body']['S']) for item in db_resp['Items']]
        return subscriptions

    def get_items_for_replica(self, replica: Replica) -> list:
        db_resp = dynamodb.scan(TableName=self.db_table_template.format(replica.name))
        subscriptions = [json.loads(item['body']['S']) for item in db_resp['Items']]
        return subscriptions

    def delete_item(self, replica: Replica, owner: str, key: str):
        dynamodb.delete_item(
            TableName=self.db_table_template.format(replica.name),
            Key={
                'hash_key': {
                    'S': owner
                },
                'sort_key': {
                    'S': key
                }
            }
        )
