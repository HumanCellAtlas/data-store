
import os
import boto3
import botocore
import pytz
from datetime import datetime
from uuid import uuid4
from ...util.aws import ARN


_deployment_stage = os.getenv('DSS_DEPLOYMENT_STAGE', 'dev')


def get_executions(arn, start_date = pytz.utc.localize(datetime.min), max_api_calls=20):

    k_api_calls = 1

    resp = boto3.client('stepfunctions').list_executions(
        stateMachineArn = arn
    )

    executions = resp['executions']

    while resp.get('nextToken', None) and executions[-1]['startDate'] > start_date:

        k_api_calls += 1

        resp = boto3.client('stepfunctions').list_executions(
            stateMachineArn = arn,
            nextToken = resp['nextToken']
        )

        executions.extend(
            resp['executions']
        )

        if k_api_calls >= max_api_calls:
            raise Exception(
                'maximum number of API calls exceeded'
            )

    executions = [
        e for e in executions
            if e['startDate'] > start_date
    ]

    return executions, k_api_calls


def get_start_date(execution_arn):

    resp = boto3.client('stepfunctions').describe_execution(
        executionArn = execution_arn
    )
    
    return resp['startDate']


def statefunction_arn(stf_name, execution_name=None):

    arn = 'arn:aws:states:{}:{}:{}:{}-{}'.format(
        ARN.get_region(),
        ARN.get_account_id(),
        'execution' if execution_name else 'stateMachine',
        stf_name,
        _deployment_stage
    )

    if execution_name is not None:
        arn += f':{execution_name}'

    return arn


def validate_bucket(bucket):
    boto3.resource('s3').meta.client.head_bucket(
        Bucket = bucket
    )
