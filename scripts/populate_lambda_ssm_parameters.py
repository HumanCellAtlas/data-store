#!/usr/bin/env python
"""
This file compiles $EXPORT_ENV_VARS_TO_LAMBDA into a json document and
uploads it into AWS Systems Manager Parameter Store under the key
`dcp/dss/{DSS_DEPLOYMENT_STAGE}/environment`, and optionally updates
the environment of every deployed lambda.
"""
import os
import sys
import json
import boto3
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--update-deployed-lambdas",
    default=False,
    action="store_true",
    help="update the environment of all deployed lambdas"
)
args = parser.parse_args()

ssm_client = boto3.client("ssm")
es_client = boto3.client("es")
lambda_client = boto3.client("lambda")

parms = {var: os.environ[var]
         for var in os.environ['EXPORT_ENV_VARS_TO_LAMBDA'].split()}
parms['DSS_ES_ENDPOINT'] = es_client.describe_elasticsearch_domain(DomainName=os.environ['DSS_ES_DOMAIN'])

ssm_client.put_parameter(
    Name=f"/dcp/dss/{os.environ['DSS_DEPLOYMENT_STAGE']}/environment",
    Value=json.dumps(parms),
    Type="String",
    Overwrite=True,
)

if args.update_deployed_lambdas:
    root, dirs, files = next(os.walk(os.path.join(os.environ['DSS_HOME'], "daemons")))
    functions = [f"{name}-{os.environ['DSS_DEPLOYMENT_STAGE']}" for name in dirs]
    functions.append(f"dss-{os.environ['DSS_DEPLOYMENT_STAGE']}") 
    for name in functions:
        try:
            resp = lambda_client.get_function(FunctionName=name)
        except lambda_client.exceptions.ResourceNotFoundException:
            print(f"{name} not deployed, or does not deploy a Lambda function")
            continue
        print(f"Updating {name}")
        lambda_client.update_function_configuration(FunctionName=name, Environment={'Variables': parms})
