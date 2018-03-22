#!/usr/bin/env python
import os
import click
import boto3

SSM = boto3.client('ssm')
access_key_id_parameter_name = os.environ['DSS_EVENT_RELAY_AWS_ACCESS_KEY_ID_PARAMETER_NAME']
secret_access_key_parameter_name = os.environ['DSS_EVENT_RELAY_AWS_SECRET_ACCESS_KEY_PARAMETER_NAME']

@click.command()
@click.argument('access_key_id')
@click.argument('secret_access_key')
def create(access_key_id, secret_access_key):
    SSM.put_parameter(
        Name=access_key_id_parameter_name,
        Value=f'{access_key_id}',
        Type='SecureString',
        Overwrite=True
    )
    SSM.put_parameter(
        Name=secret_access_key_parameter_name,
        Value=f'{secret_access_key}',
        Type='SecureString',
        Overwrite=True
    )

create()
