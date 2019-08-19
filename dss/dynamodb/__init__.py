from typing import Generator

from dss.util.aws.clients import dynamodb as db  # type: ignore


class DynamoDBItemNotFound(Exception):
    pass


def _format_item(hash_key: str, sort_key: str, value: str, ttl: int=None):
    item = {'hash_key': {'S': hash_key}}
    if value:
        item['body'] = {'S': value}
    if sort_key:
        item['sort_key'] = {'S': sort_key}
    if ttl:
        item['ttl'] = {'N': str(ttl)}
    return item


def put_item(*, table: str, hash_key: str, sort_key: str=None, value: str, dont_overwrite: str=None, ttl: int=None):
    """
    Put an item into a dynamoDB table.

    Will determine the type of db this is being called on by the number of keys provided (omit
    sort_key to PUT into a db with only 1 primary key).

    :param table: Name of the table in AWS.
    :param str value: Value stored by the two primary keys.
    :param str hash_key: 1st primary key that can be used to fetch associated sort_keys and values.
    :param str sort_key: 2nd primary key, used with hash_key to fetch a specific value.
                         Note: If not specified, this will PUT only 1 key (hash_key) and 1 value.
    :param str dont_overwrite: Don't overwrite if this parameter exists.  For example, setting this
                               to 'sort_key' won't overwrite if that sort_key already exists in the table.
    :param int ttl: Time to Live for the item.  Only works if enabled for that specific table.
    :return: None
    """
    query = {'TableName': table,
             'Item': _format_item(hash_key=hash_key, sort_key=sort_key, value=value, ttl=ttl)}
    if dont_overwrite:
        query['ConditionExpression'] = f'attribute_not_exists({dont_overwrite})'
    db.put_item(**query)


def get_item(*, table: str, hash_key: str, sort_key: str=None, return_key: str='body'):
    """
    Get associated value for a given set of keys from a dynamoDB table.

    Will determine the type of db this is being called on by the number of keys provided (omit
    sort_key to GET a value from a db with only 1 primary key).

    :param table: Name of the table in AWS.
    :param str hash_key: 1st primary key that can be used to fetch associated sort_keys and values.
    :param str sort_key: 2nd primary key, used with hash_key to fetch a specific value.
                         Note: If not specified, this will GET only 1 key (hash_key) and 1 value.
    :param str return_key: Either "body" (to return all values) or "sort_key" (to return all 2nd primary keys).
    :return: None or str
    """
    query = {'TableName': table,
             'Key': _format_item(hash_key=hash_key, sort_key=sort_key, value=None)}
    item = db.get_item(**query).get('Item')
    if item is None:
        raise DynamoDBItemNotFound(f'Query failed to fetch item from database: {query}')
    return item[return_key]['S']


def get_primary_key_items(*, table: str, key: str, return_key: str='body') -> Generator[str, None, None]:
    """
    Get associated value for a given set of keys from a dynamoDB table.

    :param table: Name of the table in AWS.
    :param str key: 1st primary key that can be used to fetch associated sort_keys and values.
    :param str return_key: Either "body" (to return all values) or "sort_key" (to return all 2nd primary keys).
    :return: Iterable (str)
    """
    paginator = db.get_paginator('query')
    for db_resp in paginator.paginate(TableName=table,
                                      ScanIndexForward=False,  # True = ascending, False = descending
                                      KeyConditionExpression="#hash_key=:key",
                                      ExpressionAttributeNames={'#hash_key': "hash_key"},
                                      ExpressionAttributeValues={':key': {'S': key}}):
        for item in db_resp.get('Items', []):
            yield item[return_key]['S']


def get_primary_key_count(*, table: str, key: str) -> int:
    """
    Returns the number of values associated with a primary key from a dynamoDB table.

    :param table: Name of the table in AWS.
    :param str key: 1st primary key that can be used to fetch associated sort_keys and values.
    :return: Int
    """
    res = db.query(TableName=table,
                   KeyConditionExpression="#hash_key=:key",
                   ExpressionAttributeNames={'#hash_key': "hash_key"},
                   ExpressionAttributeValues={':key': {'S': key}},
                   Select='COUNT')
    return res['Count']


def get_all_table_items(*, table: str, both_keys: bool=False):
    """
    Return all items from a dynamoDB table.

    :param table: Name of the table in AWS.
    :param str return_key: Either "body" (to return all values) or "sort_key" (to return all 2nd primary keys).
    :return: Iterable (str)
    """
    paginator = db.get_paginator('scan')
    for db_resp in paginator.paginate(TableName=table):
        for item in db_resp.get('Items', []):
            if both_keys:
                yield item['hash_key']['S'], item['sort_key']['S']
            else:
                yield item['body']['S']


def delete_item(*, table: str, hash_key: str, sort_key: str=None):
    """
    Delete an item from a dynamoDB table.

    Will determine the type of db this is being called on by the number of keys provided (omit
    sort_key to DELETE from a db with only 1 primary key).

    NOTE:
    Unless you specify conditions, DeleteItem is an idempotent operation; running it multiple times
    on the same item or attribute does not result in an error response:
    https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteItem.html

    :param table: Name of the table in AWS.
    :param str hash_key: 1st primary key that can be used to fetch associated sort_keys and values.
    :param str sort_key: 2nd primary key, used with hash_key to fetch a specific value.
                         Note: If not specified, this will DELETE only 1 key (hash_key) and 1 value.
    :return: None
    """
    query = {'TableName': table,
             'Key': _format_item(hash_key=hash_key, sort_key=sort_key, value=None)}
    db.delete_item(**query)
