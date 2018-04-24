#!/usr/bin/env python
"""
Use the AWS Lambda concurrency property to disable/enable lambda functions:
    - set concurrency to 0 to disable execution
    - remove concurrency setting to enable execution
"""
import os
import sys
import boto3
import click
import argparse


stage = os.environ['DSS_DEPLOYMENT_STAGE']
pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--stop", action="store_true")
parser.add_argument("--start", action="store_true")
args = parser.parse_args()
assert(
    (args.start or args.stop) and not (args.start and args.stop)
)


action = f"DSS {stage} START" if args.start else f"DSS {stage} STOP"
if not click.confirm(f"Are you sure you want to do this ({action})?"):
    sys.exit(0)


LAMBDA = boto3.client('lambda')


def disable_lambda(name):
    LAMBDA.put_function_concurrency(
        FunctionName=name,
        ReservedConcurrentExecutions=0
    )
    print(f"halted {name}")
    
def enable_lambda(name):
    LAMBDA.delete_function_concurrency(
        FunctionName=name,
    )
    print(f"started {name}")


for root, dirs, files in os.walk(os.path.join(pkg_root, 'daemons')):
    functions = [f'{name}-{stage}' for name in dirs]
    break
functions.append(
    f"dss-{stage}"
)


for f in functions:
    try:
        resp = LAMBDA.get_function(
            FunctionName=f,
        )
    except LAMBDA.exceptions.ResourceNotFoundException:
        # Either this daemon does not deploy a lambda, or this daemon is not deployed
        continue

    if args.start:
        enable_lambda(f)
    elif args.stop:
        disable_lambda(f)
