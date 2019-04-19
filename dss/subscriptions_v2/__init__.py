import os
import json

from dss.config import Replica
from dss.util.dynamodb import DynamoOwnershipLookup


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


class SubscriptionLookup(DynamoOwnershipLookup):
    def __init__(self):
        self.db_table_template = f"dss-subscriptions-v2-{{}}-{os.environ['DSS_DEPLOYMENT_STAGE']}"

    def put_subscription(self, doc: dict):
        self.put_item(replica=doc[SubscriptionData.REPLICA],
                      owner=doc[SubscriptionData.OWNER],
                      key=doc[SubscriptionData.UUID],
                      value=json.dumps(doc))

    def get_subscription(self, replica: Replica, owner: str, uuid: str) -> dict:
        return self.get_item(replica=replica, owner=owner, key=uuid)

    def get_subscriptions_for_owner(self, replica: Replica, owner: str) -> list:
        return self.get_items_for_owner(replica=replica, owner=owner)

    def get_subscriptions_for_replica(self, replica: Replica) -> list:
        return self.get_items_for_replica(replica=replica)

    def delete_subscription(self, replica: Replica, owner: str, uuid: str):
        self.delete_item(replica=replica, owner=owner, key=uuid)
