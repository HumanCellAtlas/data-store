#!/usr/bin/env python
import os
import json
import click
import boto3

SM = boto3.client('secretsmanager')
IAM = boto3.client('iam')

username = os.environ['EVENT_RELAY_AWS_USERNAME']
stage = os.environ['DSS_DEPLOYMENT_STAGE']
secrets_store = os.environ['DSS_SECRETS_STORE']
event_relay_secrets_name = os.environ['EVENT_RELAY_AWS_ACCESS_KEY_SECRETS_NAME']

def set_secret_value(key, val):
    try:
        resp = SM.get_secret_value(
            SecretId=key
        )
    except SM.exceptions.ResourceNotFoundException:
        resp = SM.create_secret(
            Name=key,
            SecretString=val
        )
        return

    resp = SM.update_secret(
        SecretId=key,
        SecretString=val
    )

secret_id = f'{secrets_store}/{stage}/{event_relay_secrets_name}'
secret = IAM.create_access_key(UserName=username)

set_secret_value(
    secret_id,
    json.dumps(secret)
)
