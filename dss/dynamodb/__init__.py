from typing import Generator

from dss.util.aws.clients import dynamodb as db  # type: ignore


def put_item(table: str, value: str, hash_key: str, sort_key: str=None):
    """
    Put an item into a dynamoDB table.

    Will determine the type of db this is being called on by the number of keys provided (omit
    sort_key to PUT into a db with only 1 primary key)

    :param table: Name of the table in AWS.
    :param str value: Value stored by the two primary keys.
    :param str hash_key: 1st primary key that can be used to fetch associated sort_keys and values.
    :param str sort_key: 2nd primary key, used with hash_key to fetch a specific value.
                         Note: If not specified, this will PUT only 1 key (hash_key) and 1 value.
    :return: None
    """
    if sort_key:
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
    else:
        db.put_item(
            TableName=table,
            Item={
                'key': {
                    'S': hash_key
                },
                'body': {
                    'S': value
                },
            }
        )


def get_item(table: str, hash_key: str, sort_key: str=None):
    """
    Get associated value for a given set of keys from a dynamoDB table.

    Will determine the type of db this is being called on by the number of keys provided (omit
    sort_key to GET a value from a db with only 1 primary key)

    :param table: Name of the table in AWS.
    :param str hash_key: 1st primary key that can be used to fetch associated sort_keys and values.
    :param str sort_key: 2nd primary key, used with hash_key to fetch a specific value.
                         Note: If not specified, this will GET only 1 key (hash_key) and 1 value.
    :return: None or str
    """
    if sort_key:
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
    else:
        db_resp = db.get_item(
            TableName=table,
            Key={
                'key': {
                    'S': hash_key
                }
            }
        )
    item = db_resp.get('Item')
    if item is not None:
        return item['body']['S']
    return item


def get_primary_key_items(table: str, key: str, return_key: str='body') -> Generator[str, None, None]:
    """
    Get associated value for a given set of keys from a dynamoDB table.

    :param table: Name of the table in AWS.
    :param str key: 1st primary key that can be used to fetch associated sort_keys and values.
    :param str return_key: Either "body" (to return all values) or "sort_key" (to return all 2nd primary keys).
    :return: Iterable (str)
    """
    db_resp = db.query(
        TableName=table,
        ScanIndexForward=False,  # True = ascending, False = descending
        KeyConditionExpression="#hash_key=:key",
        ExpressionAttributeNames={'#hash_key': "hash_key"},
        ExpressionAttributeValues={':key': {'S': key}}
    )
    for item in db_resp.get('Items', []):
        yield item[return_key]['S']


def get_all_table_items(table: str, return_key: str='body') -> Generator[str, None, None]:
    """
    Return all items from a dynamoDB table.

    :param table: Name of the table in AWS.
    :param str return_key: Either "body" (to return all values) or "sort_key" (to return all 2nd primary keys).
    :return: Iterable (str)
    """
    db_resp = db.scan(TableName=table)
    for item in db_resp.get('Items', []):
        yield item[return_key]['S']


def delete_item(table: str, hash_key: str, sort_key: str=None):
    """
    Delete an item from a dynamoDB table.

    :param table: Name of the table in AWS.
    :param str value: Value stored by the two primary keys.
    :param str hash_key: 1st primary key that can be used to fetch associated sort_keys and values.
    :param str sort_key: 2nd primary key, used with hash_key to fetch a specific value.
    :return: None
    """
    if sort_key:
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
    else:
        db.delete_item(
            TableName=table,
            Key={
                'key': {
                    'S': hash_key
                },
            }
        )
