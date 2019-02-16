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


def l2_health_checks(*args, **kwargs):

    def _get_es_status():
        es_status = False
        if os.environ['DSS_ES_ENDPOINT']:
            es_cleint = ElasticsearchClient().get()
            es_res = es_cleint.cluster.health()
            if es_res['status'] == 'green':
                es_status = True
        return es_status, es_res

    def _get_dynamodb_status():
        db_status = True
        stage = os.environ['DSS_DEPLOYMENT_STAGE']
        ddb_tables = ['dss-async-state-{}'.format(stage), 'dss-subscriptions-v2-aws-{}'.format(stage),
                      'dss-subscriptions-v2-gcp-{}'.format(stage)]
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
            service = discovery.build('cloudfunctions', 'v1')
            er_res = service.projects().locations().functions().get(name=er_name).execute()
            if er_res['status'] == 'ACTIVE':
                er_status = True
                er_data = er_res
        except Exception:
            er_status = False
            er_data = "Error"
        return er_status, er_data

    health_status = {'Healthy': True}
    # status_functions allow overriding // additional checks
    status_functions = {"elasticSearch": kwargs.get('get_es_status', _get_es_status),
                        "dynamoDB": kwargs.get("get_dynamodb_status", _get_dynamodb_status),
                        "eventRelay": kwargs.get("get_er_status", _get_event_relay_status)}
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
