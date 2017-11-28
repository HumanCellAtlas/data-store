
import os
import time
import json
import boto3
import botocore
import datetime
from uuid import uuid4
from ...util.aws import ARN


sf_client = boto3.client('stepfunctions')
_deployment_stage = os.getenv('DSS_DEPLOYMENT_STAGE', 'dev')


def compile_results(name, api_calls_per_second=2):

    res = list()

    walker_execs, _ = list_executions_for_sentinel(
        name
    )

    for w in throttled_iter(walker_execs,
                            api_calls_per_second):

        resp = sf_client.describe_execution(
            executionArn=w['executionArn']
        )

        res.append({
            'name': resp['name'],
            'status': resp['status'],
            'output': json.loads(
                resp.get(
                    'output',
                    '{}'
                )
            )
        })

    return res


def list_executions_for_sentinel(name):

    execution_arn = statefunction_arn(
        'dss-visitation-sentinel',
        name
    )

    walker_arn = statefunction_arn(
        'dss-visitation-walker'
    )

    walker_executions, k_api_calls = list_executions(
        walker_arn,
        get_start_date(
            execution_arn
        )
    )

    execs = [
        e for e in walker_executions
        if e['name'].endswith(name)
    ]

    return execs, k_api_calls


def min_datetime():

    class dont_care_which_timezone_is_min(datetime.tzinfo):
        def utcoffset(self, *args, **kwargs):
            return datetime.timedelta(0)

    return datetime.datetime.min.replace(
        tzinfo=dont_care_which_timezone_is_min()
    )


def list_executions(arn, start_date=min_datetime(), max_api_calls=50):

    if max_api_calls < 1:
        raise Exception('Must allow at least 1 api call')

    executions = list()
    kwargs = {
        'stateMachineArn': arn,
        'maxResults': 100
    }

    for k_api_calls in range(1, max_api_calls + 1):

        resp = sf_client.list_executions(
            ** kwargs
        )

        executions.extend(
            resp['executions']
        )

        if executions and executions[-1]['startDate'] < start_date:
            break

        if resp.get('nextToken', None):
            kwargs['nextToken'] = resp['nextToken']
            continue
        else:
            break

    executions = [
        e for e in executions
        if e['startDate'] > start_date
    ]

    return executions, k_api_calls


def get_start_date(execution_arn):

    resp = sf_client.describe_execution(
        executionArn=execution_arn
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


def throttled_iter(iterable, calls_per_second=2, chunk_size=10):
    t = time.time()
    k = 0

    for item in iterable:

        if k >= chunk_size:
            dt = time.time() - t
            if k / dt > calls_per_second:
                wait_time = k / calls_per_second - dt
                time.sleep(wait_time)

            t = time.time()
            k = 0

        k += 1
        yield item


def validate_bucket(bucket):
    boto3.resource('s3').meta.client.head_bucket(
        Bucket=bucket
    )
