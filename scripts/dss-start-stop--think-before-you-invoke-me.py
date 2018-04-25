#!/usr/bin/env python
"""
Use the AWS Lambda concurrency property to disable/enable lambda functions:
    - set concurrency to 0 to disable execution
    - remove concurrency setting to enable execution

As a consequence of this script, previously set Lambda concurrency limits
will be lost.

Asynchronously triggered lambda functions which are throttled are
automatically added to a redrive queue, and may be retried when lambdas
are restarted, depending on downtime.
"""
import os
import sys
import boto3
import click
import argparse


stage = os.environ['DSS_DEPLOYMENT_STAGE']
pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("action", choices=["start", "stop"])
args = parser.parse_args()


if "start" == args.action:
    msg = f"(DSS {stage} START) Lambdas will be restarted with default concurrency limits. Continue?"
elif "stop" == args.action:
    msg = f"(DSS {stage} STOP) Lambdas will be halted by setting concurrency=0. Continue?"
else:
    raise Exception(f"Unknown action {args.action}")

if not click.confirm(msg):
    sys.exit(0)


lambda_client = boto3.client('lambda')


def disable_lambda(name):
    lambda_client.put_function_concurrency(
        FunctionName=name,
        ReservedConcurrentExecutions=0
    )
    print(f"halted {name}")
    
def enable_lambda(name):
    lambda_client.delete_function_concurrency(
        FunctionName=name,
    )
    print(f"started {name}")


root, dirs, files = next(os.walk(os.path.join(pkg_root, 'daemons')))
functions = [f'{name}-{stage}' for name in dirs]
functions.append(f"dss-{stage}") 


for f in functions:
    try:
        resp = lambda_client.get_function(FunctionName=f)
    except lambda_client.exceptions.ResourceNotFoundException:
        print(f"{f} not deployed, or does not deploy a Lambda function")
        continue

    if "start" == args.action:
        enable_lambda(f)
    elif "stop" == args.action:
        disable_lambda(f)
    else:
        raise Exception(f"Unknown action {args.action}")
