import os

from dss import dynamodb  # type: ignore


collection_db_table = f"dss-collections-db-{os.environ['DSS_DEPLOYMENT_STAGE']}"


def put_collection(owner: str, versioned_uuid: str, permission_level: str = 'owner'):
    dynamodb.put_item(table=collection_db_table,
                      hash_key=owner,
                      sort_key=versioned_uuid,
                      value=permission_level,
                      dont_overwrite='sort_key')


def get_collection_uuids_for_owner(owner: str):
    """Returns an Iterator of uuid strings."""
    return dynamodb.get_primary_key_items(table=collection_db_table,
                                          key=owner,
                                          return_key='sort_key')


def get_all_collection_keys():
    """Returns an Iterator of (owner, uuid) for all items in the collections db table."""
    return dynamodb.get_all_table_items(table=collection_db_table, both_keys=True)


def delete_collection(owner: str, versioned_uuid: str):
    """Deletes one collection item from a database."""
    dynamodb.delete_item(table=collection_db_table,
                         hash_key=owner,
                         sort_key=versioned_uuid)


def delete_collection_uuid(owner: str, uuid: str):
    """Deletes all versions of a uuid in the database."""
    for versioned_uuid in get_collection_uuids_for_owner(owner):
        if versioned_uuid.startswith(uuid):
            dynamodb.delete_item(table=collection_db_table,
                                 hash_key=owner,
                                 sort_key=versioned_uuid)
