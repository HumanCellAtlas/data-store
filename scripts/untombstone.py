import os, re
import logging as logger

from dss import Replica, dynamodb
from dss.dynamodb import get_item, put_item, delete_item, DynamoDBItemNotFound
from dss.index.es import ElasticsearchClient
from dss.collections import owner_lookup
from dss.events.handlers.sync import dependencies_exist
from dcplib.aws.sqs import SQSMessenger, get_queue_url
from dss.storage.identifiers import DSS_BUNDLE_TOMBSTONE_REGEX as dead_template



sns = boto3.client('sns')
sns.meta.config.max_pool_connections = 100
sns_topic_run = "domovoi-s3-events-org-humancellatlas-dss-dev"
gs_topic_run = "dss-gs-events-org-humancellatlas-dss-dev"

# --------------------------------------------------------------
# untombstone bundles
# --------------------------------------------------------------

# revision of untombstone bundle 
def untombstone_bundle(uuid , version = None):
    sqs = boto3.resource('sqs')
    
    for replica in Repclia:
        if replica is Replica.aws:
            topic = sns_topic_run
        else:
            topic  = gs_topic_run
        if version is not None:
            key = f"bundle/{uuid}.{version}.dead"
        else:
            key = f"bundle/{uuid}.dead"
        if dependencies_exist(replica,replica,key) is False:
            pass
        else:
            deindex_dead_bundle(key)
            index_queue_url = get_queue_url(topic)
            with SQSMessenger(index_queue_url) as sqsm:
                sqsm.send(json.dumps(dict(replica=replica, key=key)))
            
# --------------------------------------------------------------
# bundle
# --------------------------------------------------------------



def deindex_dead_bundle(key):
    """
    Removes dead bundle and removes from es
    """

    es_client = ElasticsearchClient.get()
    fqid = key.split("/")[1]
    if re.match(dead_template, key):
        # matches dead fqid and deletes it from es
        es_client.delete_by_query(
            index="_all", body={"query": {"terms": {"_id": [fqid]}}}
        )
    logger.debug(f"removed dead bundle {fqid} from es")


# --------------------------------------------------------------
# collection
# --------------------------------------------------------------

def untombstone_collection(uuid, version=None):
    sqs = boto3.resource('sqs')
    
    for replica in Repclia:
        if replica is Replica.aws:
            topic = sns_topic_run
        else:
            topic  = gs_topic_run
        if version is not None:
            key = f"collections/{uuid}.{version}.dead"
        else:
            key = f"collections/{uuid}.dead"
        
        if dependencies_exist(replica,replica,key) is False:
            pass
        else:
            deindex_dead_collection(key)
            index_queue_url = get_queue_url(topic)
            with SQSMessenger(index_queue_url) as sqsm:
                sqsm.send(json.dumps(dict(replica=replica, key=key)))

def deindex_dead_collection(key):
    """
    Finds and deletes dead colletion and restores old collection
    """
    fqid = key.split("/")[1]

    dead_query = {
        "TableName": owner_lookup.collection_db_table,
        "Key": _format_dynamodb_item(sort_key=key,)
    }
    dynamodb.delete_item(**dead_query)
    logger.debug(f"removed collection {fqid} from dynamodb")
