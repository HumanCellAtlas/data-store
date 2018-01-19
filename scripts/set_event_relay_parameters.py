#!/usr/bin/env python

# Terraform scripts may be used to generate the access key id and secret access key.
# However, the secret access key will be either stored unencrypted in the Terraform state file
# or encrypted with the use of a PGP public key. In the latter case, decription will be
# required to use the secret access key, requiring the PGP secret key -- which would
# then need to be shared among operators.
#
# For now, it is preferable to grab the keys with an AWS IAM API call and store them encrypted
# into the SSM parameter store.
#
# Terraform aws_iam_access_key docs:
# https://www.terraform.io/docs/providers/aws/r/iam_access_key.html

import os
import sys
import click
import boto3

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.append(pkg_root)

import dss_deployment
active = dss_deployment.active()

SSM = boto3.client('ssm')
IAM = boto3.client('iam')
username = active.value('DSS_EVENT_RELAY_AWS_USERNAME')
parameter_store = active.value('DSS_PARAMETER_STORE')
access_key_id_parameter_name = active.value('DSS_EVENT_RELAY_AWS_ACCESS_KEY_ID_PARAMETER_NAME')
secret_access_key_parameter_name = active.value('DSS_EVENT_RELAY_AWS_SECRET_ACCESS_KEY_PARAMETER_NAME')

try:
    SSM.get_parameter(
        Name=f'{parameter_store}/{access_key_id_parameter_name}'
    )
except SSM.exceptions.ParameterNotFound:
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
