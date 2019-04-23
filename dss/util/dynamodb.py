from dss.util.aws.clients import dynamodb  # type: ignore


class DynamoLookup(object):
    @staticmethod
    def put_item(table: str, key1: str, key2: str, value: str):
        dynamodb.put_item(
            TableName=table,
            Item={
                'hash_key': {
                    'S': key1
                },
                'sort_key': {
                    'S': key2
                },
                'body': {
                    'S': value
                },
            }
        )

    @staticmethod
    def get_item(table: str, key1: str, key2: str) -> dict:
        db_resp = dynamodb.get_item(
            TableName=table,
            Key={
                'hash_key': {
                    'S': key1
                },
                'sort_key': {
                    'S': key2
                }
            }
        )
        return db_resp.get('Item')

    @staticmethod
    def get_primary_key_items(table: str, key: str) -> list:
        db_resp = dynamodb.query(
            TableName=table,
            ScanIndexForward=False,  # True = ascending, False = descending
            KeyConditionExpression="#hash_key=:key",
            ExpressionAttributeNames={'#hash_key': "hash_key"},
            ExpressionAttributeValues={':key': {'S': key}}
        )
        return db_resp.get('Items', [])

    @staticmethod
    def get_items_for_table(table: str) -> list:
        db_resp = dynamodb.scan(TableName=table)
        return db_resp.get('Items', [])

    @staticmethod
    def delete_item(table: str, key1: str, key2: str):
        dynamodb.delete_item(
            TableName=table,
            Key={
                'hash_key': {
                    'S': key1
                },
                'sort_key': {
                    'S': key2
                }
            }
        )
