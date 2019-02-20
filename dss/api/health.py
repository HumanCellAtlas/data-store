import os
import logging
from botocore import errorfactory
import requests
from flask import json
from googleapiclient import discovery, errors
from requests.exceptions import ConnectionError
from dss.index.es import ElasticsearchClient
from dss.util.aws.clients import dynamodb  # type: ignore

logger = logging.getLogger(__name__)


def _get_es_status(host: str = "localhost", port: int = None):
    """Checks ElasticSearch status, hosts can be specified"""
    try:
            es_status = False
        if port is not None:
            es_client = ElasticsearchClient().get()
        else:
            es_client = ElasticsearchClient()._get(host, port, 1)
        es_res = es_client.cluster.health()
    
    if es_res['status'] == 'green':
        es_status = True
    return es_status, es_res


def _get_dynamodb_status():
    """Checks dynamoDB table status, tables are explicitly specified within the function"""
    db_status = True
    stage = os.getenv("DSS_DEPLOYMENT_STAGE")
    ddb_tables = ["dss-async-state-{}".format(stage),
                  "dss-subscriptions-v2-aws-{}".format(stage),
                  "dss-subscriptions-v2-gcp-{}".format(stage)
                  ]
    ddb_table_data = dict.fromkeys(ddb_tables)
    for table in ddb_tables:
        try:
            table_res = dynamodb.describe_table(TableName=table)['Table']
            if table_res['TableStatus'] != 'ACTIVE':
                db_status = False
                ddb_table_data[table] = table_res.Table['TableStatus']
        except errorfactory.BaseClientExceptions.ClientError:
            db_status = False
            ddb_table_data[table] = 'Error'
    return db_status, ddb_table_data


def _get_event_relay_status():
    """Checks Google Cloud Function Event Relay"""
    er_status = False
    try:
        er_name = "projects/{}/locations/{}/functions/{}".format(
            os.environ["GCP_PROJECT_NAME"], os.environ["GCP_DEFAULT_REGION"],
            os.environ["DSS_EVENT_RELAY_NAME"] + os.environ["DSS_DEPLOYMENT_STAGE"])
        service = discovery.build('cloudfunctions', 'v1', cache_discovery=False)
        er_res = service.projects().locations().functions().get(name=er_name).execute()
        if er_res['status'] == 'ACTIVE':
            er_status = True
            er_data = er_res
    except errors.HttpError as err:
        logger.warning("Unable to get event-relay status: %s", err)
        er_status = False
        er_data = "Error"
    return er_status, er_data


def l2_health_checks():
    health_status = {'Healthy': True}
    status_functions = {"elasticSearch": _get_es_status,
                        "dynamoDB": _get_dynamodb_status,
                        "eventRelay": _get_event_relay_status}
    for key, functions in status_functions.items():
        try:
            status, payload = functions()
            if status is False:
                health_status['Healthy'] = False
                health_status[key] = payload
        except requests.exceptions.RequestException:
            health_status['Healthy'] = False
            health_status[key] = 'Error'
    if health_status["Healthy"] is False:
        logger.warning(health_status)
    return health_status
