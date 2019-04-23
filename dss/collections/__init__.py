import os

from dss.util.aws.clients import dynamodb  # type: ignore
from dss.util.dynamodb import DynamoLookup


class CollectionLookup(DynamoLookup):
    def __init__(self):
        # user: uuid: owner/read-only
        self.user_db_table = f"dss-collections-db-users-{os.environ['DSS_DEPLOYMENT_STAGE']}"
        # uuid: version
        self.version_db_table = f"dss-collections-db-versions-{os.environ['DSS_DEPLOYMENT_STAGE']}"

    def put_collection(self, owner: str, key: str, permission_level: str = 'owner'):
        """
        Adds a new owner associated collection to the table if it does not already exist.
        key  # {uuid}.{version}
        permission_level  # 'owner' or 'read-only'
        """
        uuid, version = key.split('.', 1)
        # create user -> collection -> permission level association if not present
        if not self.get_item(table=self.user_db_table, key1=owner, key2=uuid):
            self.put_item(table=self.user_db_table, key1=owner, key2=uuid, value=permission_level)
        # create uuid -> version association if not present
        if not self.get_item(table=self.version_db_table, key1=uuid, key2=version):
            self.put_item(table=self.version_db_table, key1=uuid, key2=version, value='')

    def get_collection(self, owner: str, uuid: str) -> str:
        """Returns the user permission level for a collection ('owner', 'read-only', or None)."""
        item = self.get_item(table=self.user_db_table, key1=owner, key2=uuid)
        if item is not None:
            return item['body']['S']
        else:
            return None

    def get_collections_for_owner(self, owner: str) -> list:
        db_resp = dynamodb.query(
            TableName=self.user_db_table,
            KeyConditionExpression="hash_key=:owner",
            ExpressionAttributeValues={':owner': {'S': owner}}
        )
        collections = []
        for uuid in db_resp.get('Items', []):
            collection = {'collection_uuid': uuid,
                          'collection_versions': []}
            for version in self.get_primary_key_items(table=self.version_db_table, key=uuid):
                collection['collection_versions'].append(version['sort_key']['S'])
            collections.append(collection)
        return collections

    def delete_collection(self, owner: str, uuid: str):
        # delete that user's association with the uuid
        if self.get_item(table=self.user_db_table, key1=owner, key2=uuid):
            self.delete_item(table=self.user_db_table, key1=owner, key2=uuid)
        # delete all versions from the uuid/version table
        for item in self.get_primary_key_items(table=self.user_db_table, key=uuid):
            self.delete_item(table=self.version_db_table, key1=uuid, key2=item['sort_key']['S'])
