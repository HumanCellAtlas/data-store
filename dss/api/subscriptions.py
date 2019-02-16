import logging

from dss.error import dss_handler
from dss.api import subscriptions_v1, subscriptions_v2

logger = logging.getLogger(__name__)

@dss_handler
def get(uuid: str, replica: str, subscription_type: str):
    if "elasticsearch" == subscription_type:
        return subscriptions_v1.get(uuid, replica)
    else:
        return subscriptions_v2.get(uuid, replica)

@dss_handler
def find(replica: str, subscription_type: str):
    if "elasticsearch" == subscription_type:
        return subscriptions_v1.find(replica)
    else:
        return subscriptions_v2.find(replica)

@dss_handler
def delete(uuid: str, replica: str, subscription_type: str):
    if "elasticsearch" == subscription_type:
        return subscriptions_v1.delete(uuid, replica)
    else:
        return subscriptions_v2.delete(uuid, replica)

@dss_handler
def put(json_request_body: dict, replica: str):
    if json_request_body.get('es_query'):
        return subscriptions_v1.put(json_request_body, replica)
    else:
        return subscriptions_v2.put(json_request_body, replica)
