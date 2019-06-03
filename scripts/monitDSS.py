import os
import boto3
import datetime
import json
import requests
from dss.util.aws.clients import cloudwatch  # type: ignore
from dss.util.aws.clients import resourcegroupstaggingapi  # type: ignore
from dss.util.aws.clients import secretsmanager  # type: ignore



def get_resource_by_tag(resource_string: str, tag_filter: list):
    dss_resources = resourcegroupstaggingapi.get_resources(ResourceTypeFilters=[resource_string],
                                                           TagFilters=tag_filter)
    return dss_resources


def get_lambda_names(stage=None):
    if stage is None:
        stage = os.getenv('DSS_DEPLOYMENT_STAGE')
    service_tags = [{"Key": "service", "Values": ["dss"]}, {"Key": "env", "Values": [stage]}]
    resource_list = get_resource_by_tag(resource_string='lambda:function', tag_filter=service_tags)
    lambda_names = [x['ResourceARN'].rsplit(':', 1)[1] for x in resource_list['ResourceTagMappingList'] if
                  stage in x['ResourceARN']]
    return lambda_names


def lambda_query(lambda_name, metric_name):
    namespace = 'AWS/Lambda'
    metric_name = metric_name
    end_time= datetime.datetime.now()
    start_time = end_time - datetime.timedelta(days=1)
    period = 43200
    statistics = ['Sum']
    dimensions = [{"Name": "FunctionName", "Value": lambda_name}]
    return {"Namespace": namespace,
            "MetricName": metric_name,
            "StartTime": start_time,
            "EndTime": end_time,
            "Period": period,
            "Statistics": statistics,
            "Dimensions": dimensions }


def summation_from_datapoints_response(response):
    temp_sum = 0.0
    if len(response['Datapoints']) is not 0:
        for x in response['Datapoints']:
            temp_sum += x['Sum']
        return temp_sum
    return 0.0


def send_slack_post(webhook:str, stages:dict):
    payload = {'text': json.dumps(stages,indent=4, sort_keys=True)}
    res = requests.post(webhook, json=payload, headers={'Content-Type': 'application/json'})
    if res.status_code != 200:
        raise ValueError(
            'Request to slack returned an error %s, the response is:\n%s'
            % (res.status_code, res.text)
        )

def get_webhook_ssm(secret_name=None):
    stage = os.environ['DSS_DEPLOYMENT_STAGE']
    secrets_store = os.environ['DSS_SECRETS_STORE']
    if secret_name is None:
        secret_name = 'monitor-webhook'
    secret_id = f'{secrets_store}/{stage}/{secret_name}'
    res = secretsmanager.get_secret_value(SecretId=secret_id)
    return res['SecretString']


stages = {'dev': None, 'staging': None, 'integration': None}

for stage in stages.keys():
    stage_lambdas = {i: {} for i in get_lambda_names(stage)}
    for ln in stage_lambdas.keys():
        duration_res = cloudwatch.get_metric_statistics(**lambda_query(ln, 'Duration'))
        invocation_res = cloudwatch.get_metric_statistics(**lambda_query(ln, 'Invocations'))
        stage_lambdas[ln]['Duration'] = summation_from_datapoints_response(duration_res)/1000  # x/1000 for seconds conversion
        stage_lambdas[ln]['Invocations'] = summation_from_datapoints_response(invocation_res)
    stages[stage] = stage_lambdas
send_slack_post(get_webhook_ssm(), stages)


