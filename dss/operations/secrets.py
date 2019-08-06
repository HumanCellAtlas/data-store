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


@events.action("list-secrets")
def list_secrets(argv: typing.List[str], args: argparse.Namespace):
    """Print a list of all secrets"""
    pass

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
