import os
import logging
from botocore import errorfactory
import requests
from flask import json
from googleapiclient import discovery, errors
from elasticsearch.exceptions import ConnectionError
from dss.index.es import ElasticsearchClient
from dss.util.aws.clients import dynamodb  # type: ignore
from dss.util.aws.clients import resourcegroupstaggingapi  # type: ignore

logger = logging.getLogger(__name__)


def get_resource_by_tag(resource_string: str, tag_filter: dict):
    dss_resources = resourcegroupstaggingapi.get_resources(ResourceTypeFilters=[resource_string],
                                                           TagFilters=[tag_filter])
    return dss_resources


def _get_es_status(host: str = "localhost", port: int = None):
    """Checks ElasticSearch status, hosts can be specified"""
    try:
        es_status = False
        es_res = {"status": ""}
        if port is not None:
            es_client = ElasticsearchClient().get()
        else:
            es_client = ElasticsearchClient()._get(host, port, 1)
        es_res = es_client.cluster.health()
    except ConnectionError as exception:
        logger.warning("connection error with ElasticSearch: %s" % exception)
        es_res['status'] = 'red'
    if es_res['status'] == 'green':
        es_status = True
    return es_status, es_res


def _get_dynamodb_status():
    """Checks dynamoDB table status, tables are explicitly specified within the function"""
    db_status = True
    stage = os.getenv("DSS_DEPLOYMENT_STAGE")
    service_tags = {"Key": "service", "Values": ["dss"]}
    resource_list = get_resource_by_tag(resource_string='dynamodb:table', tag_filter=service_tags)
    ddb_tables = [x['ResourceARN'].split('/')[1] for x in resource_list['ResourceTagMappingList'] if
                  stage in x['ResourceARN']]
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
            os.environ["GCP_PROJECT_ID"], os.environ["GCP_DEFAULT_REGION"],
            os.environ["DSS_EVENT_RELAY_NAME"] + os.environ["DSS_DEPLOYMENT_STAGE"])
        service = discovery.build('cloudfunctions', 'v1', cache_discovery=False)
        er_res = service.projects().locations().functions().get(name=er_name).execute()
        if er_res['status'] == 'ACTIVE':
            er_status = True
    except errors.HttpError as err:
        logger.warning("Unable to get event-relay status: %s" % err)
        er_status = False
        er_res = "Error"
    return er_status, er_res


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
