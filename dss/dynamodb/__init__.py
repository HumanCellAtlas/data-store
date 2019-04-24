from dss.util.aws.clients import dynamodb as db  # type: ignore


def put_item(table: str, value: str, hash_key: str, sort_key: str=None):
    db.put_item(
        TableName=table,
        Item={
            'hash_key': {
                'S': hash_key
            },
            'sort_key': {
                'S': sort_key
            },
            'body': {
                'S': value
            },
        }
    )


def get_item(table: str, hash_key: str, sort_key: str=None):
    db_resp = db.get_item(
        TableName=table,
        Key={
            'hash_key': {
                'S': hash_key
            },
            'sort_key': {
                'S': sort_key
            }
        }
    )
    item = db_resp.get('Item')
    if item is not None:
        return item['body']['S']
    return item


def get_primary_key_items(table: str, key: str, return_key: str='body') -> list:
    db_resp = db.query(
        TableName=table,
        ScanIndexForward=False,  # True = ascending, False = descending
        KeyConditionExpression="#hash_key=:key",
        ExpressionAttributeNames={'#hash_key': "hash_key"},
        ExpressionAttributeValues={':key': {'S': key}}
    )
    for item in db_resp.get('Items', []):
        yield item[return_key]['S']


def get_all_table_items(table: str) -> list:
    db_resp = db.scan(TableName=table)
    return db_resp.get('Items', [])


def delete_item(table: str, hash_key: str, sort_key: str=None):
    db.delete_item(
        TableName=table,
        Key={
            'hash_key': {
                'S': hash_key
            },
            'sort_key': {
                'S': sort_key
            }
        }
    )
