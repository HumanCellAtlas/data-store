import os
import json

from dss.config import Replica
from dss.util.dynamodb import DynamoLookup


class SubscriptionData:
    REPLICA = 'replica'
    OWNER = 'owner'
    UUID = 'uuid'
    CALLBACK_URL = 'callback_url'
    JMESPATH_QUERY = 'jmespath_query'
    METHOD = 'method'
    ENCODING = 'encoding'
    FORM_FIELDS = 'form_fields'
    PAYLOAD_FORM_FIELD = 'payload_form_field'
    ATTACHMENTS = 'attachments'


class SubscriptionLookup(DynamoLookup):
    def __init__(self):
        self.db_table_template = f"dss-subscriptions-v2-{{}}-{os.environ['DSS_DEPLOYMENT_STAGE']}"

    def put_subscription(self, doc: dict):
        self.put_item(table=self.db_table_template.format(doc[SubscriptionData.REPLICA].name),
                      key1=doc[SubscriptionData.OWNER],
                      key2=doc[SubscriptionData.UUID],
                      value=json.dumps(doc))

    def get_subscription(self, replica: Replica, owner: str, uuid: str):
        item = self.get_item(table=self.db_table_template.format(replica.name),
                             key1=owner,
                             key2=uuid)
        if item is not None:
            return json.loads(item['body']['S'])
        else:
            return None

    def get_subscriptions_for_owner(self, replica: Replica, owner: str) -> list:
        items = self.get_primary_key_items(table=self.db_table_template.format(replica.name),
                                           key=owner)
        return [json.loads(item['body']['S']) for item in items]

    def get_subscriptions_for_replica(self, replica: Replica) -> list:
        items = self.get_items_for_table(table=self.db_table_template.format(replica.name))
        return [json.loads(item['body']['S']) for item in items]

    def delete_subscription(self, replica: Replica, owner: str, uuid: str):
        self.delete_item(table=self.db_table_template.format(replica.name),
                         key1=owner,
                         key2=uuid)
