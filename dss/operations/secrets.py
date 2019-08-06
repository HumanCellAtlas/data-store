"""
Get/set secret variable values from the AWS Secrets Manager
"""
import os
import sys
import click
import boto3
import select
import typing
import argparse
import json
import logging
import subprocess

from dss.operations import dispatch


logger = logging.getLogger(__name__)


events = dispatch.target("secrets",
                         help=__doc__)


def get_secret_names(sm_client):
    """
    This retrieves a list of names of all secret variables 
    in the secret manager
    """
    # Also see boto docs:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ssm.html#SSM.Client.describe_parameters
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ssm.html#SSM.Paginator.DescribeParameters
    secret_names = []

    # Create a paginator from the describe_parameters endpoint
    # and use it to get the name of each parameter
    paginator = sm.get_paginator('describe_parameters')
    for response in paginator.paginate():
        while response['NextToken'] != '':
            for param in response['Parameters']:
                secret_names.append(param['Name'])

    return secret_names

@events.action("name-secrets")
def name_secrets(argv: typing.List[str], args: argparse.Namespace):
    """Print the names of all secret variables"""
    sm = boto3.client('ssm')

    # Print the name of each secret
    names = get_secret_names(sm)
    for secret_name in secret_names:
        print("{}".format(secret_name))

@events.action("list-secrets")
def list_secrets(argv: typing.List[str], args: argparse.Namespace):
    """Print a list of all secrets"""
    sm = boto3.client('ssm')

    # Get the name of each secret
    secret_names = get_secret_names(sm)

    # Use the name of each secret to retrieve/print its value
    for secret_name in secret_names:
        response = sm.get_parameter(Name=secret_name)
        print("{} = {}".format(response['Parameter']['Name'], response['Parameter']['Value']))

@events.action("get-secret",
               arguments={
                   "--secret-name": dict(
                       required=True,
                       nargs="*",
                       help="name of secret to retrieve")})
def get_secret(argv: typing.List[str], args: argparse.Namespace):
    """Get the value of the secret variable specified by the --secret-name flag"""
    sm = boto3.client('secretsmanager')
    stage = os.environ['DSS_DEPLOYMENT_STAGE']
    secrets_store = os.environ['DSS_SECRETS_STORE']
    secret_id = f'{secrets_store}/{stage}/{args.secret_name}'

    try:
        # Start by trying to get the secret variable
        secret_val = sm.get_secret_value(SecretId=secret_id)

    except sm.exceptions.ResourceNotFoundException:
        # The secret variable does not exist
        print("Resource Not Found: {}".format(secret_id))

    else:
        # Get operation was successful, secret variable exists
        print('Resource Found: {} = {}'.format(secret_id, secret_val))

@events.action("set-secret",
               arguments={
                   "--secret-name": dict(required=True,
                                         nargs="*",
                                         help="name of secret to retrieve"),
                   "--dry-run": dict(help="do a dry run of the actual operation")})
def set_secret(argv: typing.List[str], args: argparse.Namespace):
    """Set the value of the secret variable specified by the --secret-name flag"""
    sm = boto3.client('secretsmanager')
    stage = os.environ['DSS_DEPLOYMENT_STAGE']
    secrets_store = os.environ['DSS_SECRETS_STORE']
    secret_id = f'{secrets_store}/{stage}/{args.secret_name}'

    # Use the `select` module to obtain the value that is passed
    # to this script via stdin (i.e., piped to this script).
    # Note: user provides secret variable *value* via stdin,
    # user provides secret variable *name* via --secret-name flag.
    if not select.select([sys.stdin, ], [], [], 0.0)[0]:
        print(f"No data in stdin, exiting without setting {secret_id}")
        sys.exit()
    val = sys.stdin.read()

    print("Setting", secret_id)

    try:
        # Start by trying to get the secret variable
        _ = sm.get_secret_value(SecretId=secret_id)

    except sm.exceptions.ResourceNotFoundException:
        # The secret variable does not exist, so create it
        if args.dry_run:
            # Create it for fakes
            print("Resource Not Found: Creating {}".format(secret_id))
        else:
            # Create it for real
            _ = sm.create_secret(
                Name=secret_id,
                SecretString=val
            )

    else:
        # Get operation was successful, secret variable exists
        if args.dry_run:
            print('Resource Found: Updating {}'.format(secret_id))
        else:
            _ = sm.update_secret(
                SecretId=secret_id,
                SecretString=val
            )
