#!/usr/bin/env python
"""
This script idempotently maps the API Gateway custom domain name to the API Gateway
stage. It should be executed for a first-time deployment after successfully
running `make deploy-infra` and `make deploy`.
"""

import os
import sys
import json
import boto3

stage = os.environ['DSS_DEPLOYMENT_STAGE']
domain_name = os.environ['API_DOMAIN_NAME']

APIGATEWAY = boto3.client('apigateway')
LAMBDA = boto3.client('lambda')
lambda_name = f'dss-{stage}'

lambda_arn = None
paginator = LAMBDA.get_paginator('list_functions')
for page in paginator.paginate():
    for l in page['Functions']:
        if lambda_name == l['FunctionName']:
            lambda_arn = l['FunctionArn']
            break

if not lambda_arn:
    raise Exception(f'Lambda function {lambda_name} not found. Did you run `make deploy`?')

policy = json.loads(
    LAMBDA.get_policy(FunctionName=lambda_name)['Policy']
)

source_arn = policy['Statement'][0]['Condition']['ArnLike']['AWS:SourceArn']
api_id = source_arn.split(':')[5].split('/')[0]

try:
    APIGATEWAY.create_base_path_mapping(
        domainName=domain_name,
        restApiId=api_id,
        stage=stage
    )
except APIGATEWAY.exceptions.ConflictException:
    pass
