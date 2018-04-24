#!/usr/bin/env python
import os
import click
import boto3

SM = boto3.client('secretsmanager')
IAM = boto3.client('iam')
username = os.environ['DSS_EVENT_RELAY_AWS_USERNAME']
secrets_store = os.environ['DSS_SECRETS_STORE']
access_key_id_secrets_name = os.environ['DSS_EVENT_RELAY_AWS_ACCESS_KEY_ID_SECRETS_NAME']
secret_access_key_secrets_name = os.environ['DSS_EVENT_RELAY_AWS_SECRET_ACCESS_KEY_SECRETS_NAME']

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

key_info = IAM.create_access_key(
    UserName=username
)

set_secret_value(
    f'{secrets_store}/{access_key_id_secrets_name}',
    key_info['AccessKey']['AccessKeyId']
)

set_secret_value(
    f'{secrets_store}/{secret_access_key_secrets_name}',
    key_info['AccessKey']['SecretAccessKey']
)
