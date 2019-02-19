import datetime
import os
import boto3
from botocore import errorfactory
import requests
from flask import json
import sys

from dss.index.es import ElasticsearchClient

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'chalicelib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from googleapiclient import discovery
from dss.util.aws.clients import dynamodb  # type: ignore


def _get_es_status(host: str = "localhost", port: int = None):
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
    db_status = True
    ddb_tables = list(filter(lambda x: x.startswith("dss") and x.endswith(os.getenv("DSS_DEPLOYMENT_STAGE")),
                             dynamodb.list_tables()["TableNames"]))
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
    er_status = False
    try:
        er_name = "projects/{}/locations/{}/functions/{}".format(
            os.environ["GCP_PROJECT_NAME"], os.environ["GCP_DEFAULT_REGION"], os.environ["DSS_EVENT_RELAY_NAME"])
        service = discovery.build('cloudfunctions', 'v1', cache_discovery=False)
        er_res = service.projects().locations().functions().get(name=er_name).execute()
        if er_res['status'] == 'ACTIVE':
            er_status = True
            er_data = er_res
    except Exception:
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
            pass
    return json.dumps(health_status, indent=4, sort_keys=True, default=str)
