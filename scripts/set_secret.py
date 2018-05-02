#!/usr/bin/env python
import os
import sys
import click
import boto3
import select
import argparse


SM = boto3.client('secretsmanager')
stage = os.environ['DSS_DEPLOYMENT_STAGE']
secrets_store = os.environ['DSS_SECRETS_STORE']


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--secret-name", required=True)
args = parser.parse_args()


secret_id = f'{secrets_store}/{stage}/{args.secret_name}'


if not select.select([sys.stdin,],[],[],0.0)[0]:
    print(f"No data in stdin, exiting without setting {secret_id}")
    sys.exit()
val = sys.stdin.read()


print("setting", secret_id)


try:
    resp = SM.get_secret_value(
        SecretId=secret_id
    )
except SM.exceptions.ResourceNotFoundException:
    resp = SM.create_secret(
        Name=secret_id,
        SecretString=val
    )
else:
    resp = SM.update_secret(
        SecretId=secret_id,
        SecretString=val
    )
