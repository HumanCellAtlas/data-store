#!/usr/bin/env python
import os
import click
import boto3

SSM = boto3.client('ssm')
IAM = boto3.client('iam')
username = os.environ['DSS_EVENT_RELAY_AWS_USERNAME']
parameter_store = os.environ['DSS_PARAMETER_STORE']
access_key_id_parameter_name = os.environ['DSS_EVENT_RELAY_AWS_ACCESS_KEY_ID_PARAMETER_NAME']
secret_access_key_parameter_name = os.environ['DSS_EVENT_RELAY_AWS_SECRET_ACCESS_KEY_PARAMETER_NAME']

key_info = IAM.create_access_key(
    UserName=username
)

SSM.put_parameter(
    Name=f'{parameter_store}/{access_key_id_parameter_name}',
    Value=key_info['AccessKey']['AccessKeyId'],
    Type='SecureString',
    Overwrite=True
)

SSM.put_parameter(
    Name=f'{parameter_store}/{secret_access_key_parameter_name}',
    Value=key_info['AccessKey']['SecretAccessKey'],
    Type='SecureString',
    Overwrite=True
)
