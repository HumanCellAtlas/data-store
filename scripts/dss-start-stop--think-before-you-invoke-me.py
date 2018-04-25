#!/usr/bin/env python
"""
Use the AWS Lambda concurrency property to disable/enable lambda functions:
    - set concurrency to 0 to disable execution
    - remove concurrency setting to enable execution

As a consequence of this script, previously set Lambda concurrency limits
will be lost.
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


action = f"DSS {stage} START" if args.action=="start" else f"DSS {stage} STOP"
if not click.confirm(f"Are you sure you want to do this ({action})?"):
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

    if args.start:
        enable_lambda(f)
    elif args.stop:
        disable_lambda(f)
