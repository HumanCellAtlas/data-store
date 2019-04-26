import os

from dss import dynamodb  # type: ignore


collection_db_table = f"dss-collections-db-{os.environ['DSS_DEPLOYMENT_STAGE']}"


def put_collection(owner: str, uuid: str, permission_level: str = 'owner'):
    dynamodb.put_item(table=collection_db_table,
                      hash_key=owner,
                      sort_key=uuid,
                      value=permission_level,
                      dont_overwrite='sort_key')


def get_collection_uuids_for_owner(owner: str) -> list:
    items = dynamodb.get_primary_key_items(table=collection_db_table,
                                           key=owner,
                                           return_key='sort_key')
    return [item for item in items]


def get_all_collection_uuids():
    items = dynamodb.get_all_table_items(table=collection_db_table, return_key='sort_key')
    return items


def delete_collection(owner: str, uuid: str):
    dynamodb.delete_item(table=collection_db_table,
                         hash_key=owner,
                         sort_key=uuid)
