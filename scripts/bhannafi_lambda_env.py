#!/usr/bin/env python
"""
Set/unset an env variable on a deployed Lambda function
e.g. `scripts/bhannafi_lambda_env.py dss set DSS_READ_ONLY_MODE --value True`
e.g. `scripts/bhannafi_lambda_env.py dss unset DSS_READ_ONLY_MODE`
"""
import sys
import boto3
import argparse


lambda_client = boto3.client('lambda')


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("lambda_name")
parser.add_argument("action", choices=["set", "unset"])
parser.add_argument("key")
parser.add_argument("--value", required=False)
parser.add_argument("-d", "--stage", default="dev", choices=["dev", "integration", "staging", "prod"])
args = parser.parse_args()

function_name=f"{args.lambda_name}-{args.stage}"
lambda_env = lambda_client.get_function_configuration(FunctionName=function_name)['Environment']

if "unset" == args.action:
    del lambda_env['Variables'][args.key]
elif "set" == args.action:
    lambda_env['Variables'][args.key] = args.value
else:
    raise Exception("Unknown action")

lambda_client.update_function_configuration(FunctionName=function_name, Environment=lambda_env)
