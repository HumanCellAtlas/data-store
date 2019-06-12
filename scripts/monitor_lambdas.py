#! /usr/bin/env python

import os
import boto3
import datetime
import json
import requests
import argparse
import collections


cloudwatch = boto3.client('cloudwatch')
resourcegroupstaggingapi = boto3.client('resourcegroupstaggingapi')
secretsmanager = boto3.client('secretsmanager')
logsmanager = boto3.client('logs')


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--no-webhook", required=False, action='store_true',
                    help='does not push to webhook, outputs json to screen')
args = parser.parse_args()


def get_webhook_ssm(secret_name=None):
    #  fetch webhook url from Secrets Store.
    stage = os.environ['DSS_DEPLOYMENT_STAGE']
    secrets_store = os.environ['DSS_SECRETS_STORE']
    if secret_name is None:
        secret_name = 'monitor-webhook'
    secret_id = f'{secrets_store}/{stage}/{secret_name}'
    res = secretsmanager.get_secret_value(SecretId=secret_id)
    return res['SecretString']


def get_resource_by_tag(resource_string: str, tag_filter: list):
    dss_resources = resourcegroupstaggingapi.get_resources(ResourceTypeFilters=[resource_string],
                                                           TagFilters=tag_filter)
    return dss_resources


def get_lambda_names(stage=None):
    # Returns all the names for deployed lambdas
    if stage is None:
        stage = os.getenv('DSS_DEPLOYMENT_STAGE')
    service_tags = [{"Key": "service", "Values": ["dss"]}, {"Key": "env", "Values": [stage]}]
    resource_list = get_resource_by_tag(resource_string='lambda:function', tag_filter=service_tags)
    lambda_names = [x['ResourceARN'].rsplit(':', 1)[1] for x in resource_list['ResourceTagMappingList'] if
                  stage in x['ResourceARN']]
    return sorted(lambda_names)


def get_cloudwatch_metric_stat(namespace: str, metric_name: str, stats: list, dimensions):
    #  Returns a formatted MetricDataQuery that can be used with CloudWatch Metrics
    end_time = aws_end_time
    start_time = aws_start_time
    period = 43200
    if not stats:
        stats = ['Sum']
    return {"Namespace": namespace,
            "MetricName": metric_name,
            "StartTime": start_time,
            "EndTime": end_time,
            "Period": period,
            "Statistics": stats,
            "Dimensions": dimensions}


def summation_from_datapoints_response(response):
    # Datapoints from CloudWatch queries may need to be summed due to how durations for time delta's are calculated.
    temp_sum = 0.0
    if len(response['Datapoints']) is not 0:
        for x in response['Datapoints']:
            temp_sum += x['Sum']
        return temp_sum
    return 0.0


def format_lambda_results_for_slack(results: dict):
    # Formats json lambda data into something that can be presented in slack
    header = '\n {} : {} -> {} | \n  Lambda Name | Invocations | Duration (seconds) \n'
    bucket_header = '\n Bucket | BytesUploaded | BytesDownloaded \n'
    payload = []
    for stage, infra in results.items():
        temp_results_lambdas = [header.format(stage, aws_start_time, aws_end_time)]
        temp_results_buckets = [bucket_header]
        for k, v in infra.items():
            if 'lambdas' in k:
                for ln, val in v.items():
                    temp_results_lambdas.append(f'\n\t | {ln} | {val["Invocations"]} | {val["Duration"]/1000} ')
            elif 'buckets' in k:
                for bn, val in v.items():
                    temp_results_buckets.append(f'\n\t | {bn} | {format_data_size(val["BytesUploaded"])} | '
                                                f'{format_data_size(val["BytesDownloaded"])}')
        payload.append(''.join(temp_results_lambdas+temp_results_buckets))

    return ''.join(payload)


def send_slack_post(webhook:str, stages: dict):
    payload = {"text": f"{format_lambda_results_for_slack(stages)}"}
    res = requests.post(webhook, json=payload, headers={'Content-Type': 'application/json'})
    if res.status_code != 200:
        raise ValueError(
            'Request to slack returned an error %s, the response is:\n%s'
            % (res.status_code, res.text)
        )


def format_data_size(value: int):
    base = 1000
    value = float(value)
    suffix = ('kB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB')
    if value < base:
        return '%d Bytes' % value
    for i, s in enumerate(suffix):
        unit = base ** (i + 2)
        if value < unit:
            return (format + ' %s') % ((base * value / unit), s)
        elif value < unit:
            return (format + '%s') % ((base * value / unit), s)


def get_cloudwatch_log_events(group_name: str, filter_pattern: str, token: str = None):
        epoch = datetime.datetime.utcfromtimestamp(0)
        events = []

        # TODO fix timing 
        kwargs = {'endTime': int((aws_end_time - epoch).total_seconds()*1000),
                  'startTime': int((aws_start_time - epoch).total_seconds()*1000),
                  'logGroupName': group_name, 'filterPattern': filter_pattern, 'interleaved': True}
        if token:
            kwargs['nextToken'] = token
        res = logsmanager.filter_log_events(**kwargs)
        print(res)
        if len(res["events"]) > 0:
            print(f'found events : {len(res["events"])}')
            events.extend(res["events"])

        if res['nextToken']:
            print('recurse')
            events.extend(get_cloudwatch_log_events(group_name, filter_pattern, token))
        return events

# conditionals
if os.environ["DSS_DEPLOYMENT_STAGE"] is None:
    raise ValueError('Missing DSS_DEPLOYMENT_STAGE, exiting....')
    exit(1)

# variables
aws_end_time = datetime.datetime.utcnow()
aws_start_time = aws_end_time - datetime.timedelta(days=1)
bucket_list = [os.environ['DSS_S3_BUCKET'], os.environ['DSS_S3_CHECKOUT_BUCKET']]
bucket_query_metric_names = ['BytesDownloaded', 'BytesUploaded']
lambda_query_metric_names = ['Duration', 'Invocations']
stages = {f'{os.environ["DSS_DEPLOYMENT_STAGE"]}': collections.defaultdict(collections.defaultdict)}

for stage in stages.keys():
    stage_lambdas = {i: collections.defaultdict(collections.defaultdict) for i in get_lambda_names(stage)}
    for ln in stage_lambdas.keys():
        for lambda_metric in lambda_query_metric_names:
            lambda_res = cloudwatch.get_metric_statistics(**get_cloudwatch_metric_stat('AWS/Lambda',
                                                                                         lambda_metric,
                                                                                         ['Sum'],
                                                                                         [{"Name": "FunctionName",
                                                                                           "Value": ln}]))
            stage_lambdas[ln][lambda_metric] = int(summation_from_datapoints_response(lambda_res))
    stages[stage]['lambdas'].update(stage_lambdas)
    for bucket_name in bucket_list:
        #  Fetch Data for Buckets Data Consumption
        temp_dir = collections.defaultdict(int)
        for metric in bucket_query_metric_names:
            bucket_upload_res = cloudwatch.get_metric_statistics(**get_cloudwatch_metric_stat('AWS/S3',
                                                                                              metric,
                                                                                              ['Sum'],
                                                                                              [{"Name": "BucketName",
                                                                                                "Value": bucket_name},
                                                                                               {"Name": "FilterId",
                                                                                                "Value": "EntireBucket"}]))
            temp_dir[metric] = int(summation_from_datapoints_response(bucket_upload_res))
        stages[stage]['buckets'].update({bucket_name: temp_dir})
if args.no_webhook:
    print(json.dumps(stages, indent=4, sort_keys=True))
else:
    send_slack_post(get_webhook_ssm(), stages)
