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

from dss.operations import dispatch
from dss.util.aws import ARN as arn
from dss.util.aws.clients import secretsmanager


logger = logging.getLogger(__name__)


def get_long_name(secret_name, arn_prefix, store_prefix):
    """
    Given a (user-provided) name of a secret variable,
    determine whether the ARN prefix and store/stage
    prefix must be added to that variable to get its
    long name.

    Users can specify variable names in any of the
    following forms, and this function will return
    the full ARN resource name:

    - es_source_ip-q2baD1 (no store/stage prefix present)
    - dcp/dss/dev/es_source_ip-q2baD1 (no ARN prefix present)
    - arn:aws:secretsmanager:us-east-1:861229788715:secret:(continued)
      dcp/dss/dev/es_source_ip-q2baD1 (target output, store/stage prefix
      and ARN prefix present)
    """
    # Figure out what kind of prefix the user provided
    # with the provided list of list
    user_provided_arn = secret_name.startswith(arn_prefix)
    user_provided_store = secret_name.startswith(store_prefix)

    # Add any prefix that is needed, get the full ARN
    if user_provided_arn:
        full_secret_name = secret_name
    elif user_provided_store:
        full_secret_name = arn_prefix + secret_name
    else:
        full_secret_name = arn_prefix + store_prefix + secret_name

    return full_secret_name


def get_short_name(secret_name, arn_prefix, store_prefix):
    """
    Given a (user-provided) name of a secret variable,
    determine whether the ARN prefix and store/stage
    prefix must be removed from that variable to get
    its short name.

    Users can specify variable names in any of the
    following forms, and this function will return
    the store/stage prefixed resource name only:

    - es_source_ip-q2baD1 (no store/stage prefix present)
    - dcp/dss/dev/es_source_ip-q2baD1 (target output)
    - arn:aws:secretsmanager:us-east-1:861229788715:secret:dcp/dss/dev/es_source_ip-q2baD1 (ARN prefix present)
    """
    # Figure out what kind of prefix the user provided
    # with the provided list of list
    user_provided_arn = secret_name.startswith(arn_prefix)
    user_provided_store = secret_name.startswith(store_prefix)

    # Remove any prefix that is needed, get the store/stage prefixed name
    if user_provided_arn:
        short_secret_name = secret_name[len(arn_prefix):]
    elif user_provided_store:
        short_secret_name = secret_name
    else:
        short_secret_name = store_prefix + secret_name

    return short_secret_name


events = dispatch.target("secrets",
                         arguments={},
                         help=__doc__)

@events.action("list",
               arguments={
                   "--stage": dict(
                       required=False,
                       help="the stage for which secrets should be listed"),
                   "--long": dict(
                       default=False,
                       action="store_true",
                       help="use long identifiers (incl. ARN, region, project ID) for resources"),
                   "--json": dict(
                       default=False,
                       action="store_true",
                       help="format the output as JSON if this flag is present")})
def list_secrets(argv: typing.List[str], args: argparse.Namespace):
    """
    Print a list of names of every secret variable in the secrets manager
    for the given store and stage
    """
    store_name = os.environ['DSS_SECRETS_STORE']
    if args.stage is None:
        stage_name = os.environ['DSS_DEPLOYMENT_STAGE']

    paginator = secretsmanager.get_paginator('list_secrets')

    prefix = f'{store_name}/{stage_name}/'
    secret_names = []
    for response in paginator.paginate():
        for secret in response['SecretList']:

            long_name = secret['ARN']
            short_name = long_name.split(":")[-1]
            if short_name.startswith(prefix):
                # If the prefix matches the store and stage specified,
                # store this secret's name for later
                if args.long:
                    secret_names.append(long_name)
                else:
                    secret_names.append(short_name)

    secret_names.sort()

    if args.json:
        print(json.dumps(secret_names))
    else:
        for secret_name in secret_names:
            print(secret_name)


@events.action("get",
               arguments={
                   "--stage": dict(
                       required=False,
                       help="the stage for which secrets should be listed"),
                   "--secret-name": dict(
                       required=True,
                       nargs="*",
                       help="name of secret or secrets to retrieve (list values can be separated by a space)"),
                   "--json": dict(
                       default=False,
                       action="store_true",
                       help="format the output as JSON if this flag is present")})
def get_secret(argv: typing.List[str], args: argparse.Namespace):
    """
    Get the value of the secret variable (or list of variables) specified by
    the --secret-name flag
    """
    # Note: this function should not print anything except the final JSON,
    # in case the user pipes the JSON output of this script to something else
    store_name = os.environ['DSS_SECRETS_STORE']
    if args.stage is None:
        stage_name = os.environ['DSS_DEPLOYMENT_STAGE']

    if args.secret_name is None or len(args.secret_name) == 0:
        logger.error("Unable to get secret: no secret was specified! Use the --secret-name flag.")
        sys.exit()

    # Necessary because AWS requires full resource identifiers to fetch secrets
    region_name = arn.get_region()
    account_id = arn.get_account_id()
    arn_prefix = f'arn:aws:secretsmanager:{region_name}:{account_id}:secret:'
    store_prefix = f'{store_name}/{stage_name}/'

    for secret_name in args.secret_name:
        full_secret_name = get_long_name(secret_name, arn_prefix, store_prefix)
        short_secret_name = get_short_name(full_secret_name, arn_prefix, store_prefix)

        try:
            response = secretsmanager.get_secret_value(SecretId=full_secret_name)
            secret_val = response['SecretString']
        except secretsmanager.exceptions.ResourceNotFoundException:
            # A secret variable with that name does not exist
            logger.warning("Resource not found: {}".format(full_secret_name))
        else:
            if args.json:
                print(json.dumps({full_secret_name: secret_val}))
            else:
                # Get operation was successful, secret variable exists
                print("{}={}".format(short_secret_name, secret_val))


@events.action("set",
               arguments={
                   "--stage": dict(
                       required=False,
                       help="the stage for which secrets should be set"),
                   "--secret-name": dict(
                       required=True,
                       help="name of secret to set (limit 1 at a time)"),
                   "--dry-run": dict(
                       default=False,
                       action="store_true",
                       help="do a dry run of the actual operation")})
def set_secret(argv: typing.List[str], args: argparse.Namespace):
    """Set the value of the secret variable specified by the --secret-name flag"""
    store_name = os.environ['DSS_SECRETS_STORE']
    if args.stage is None:
        stage_name = os.environ['DSS_DEPLOYMENT_STAGE']

    # Make sure a secret name was specified
    if args.secret_name is None or len(args.secret_name) == 0:
        logger.error("Unable to set secret: no secret name was specified! Use the --secret-name flag.")
        sys.exit()
    secret_name = args.secret_name

    # Use the `select` module to obtain the value that is passed
    # to this script via stdin (i.e., piped to this script).
    # Note: user provides secret variable *value* via stdin,
    # user provides secret variable *name* via --secret-name flag.
    if not select.select([sys.stdin, ], [], [], 0.0)[0]:
        err_msg = f"No data in stdin, cannot set secret {secret_name} without a value from stdin!"
        logger.error(err_msg)
        sys.exit()
    secret_val = sys.stdin.read()

    # Necessary because AWS requires full resource identifiers to fetch secrets
    region_name = arn.get_region()
    account_id = arn.get_account_id()
    arn_prefix = f'arn:aws:secretsmanager:{region_name}:{account_id}:secret:'
    store_prefix = f'{store_name}/{stage_name}/'

    full_secret_name = get_long_name(secret_name, arn_prefix, store_prefix)
    short_secret_name = get_short_name(full_secret_name, arn_prefix, store_prefix)

    try:
        # Start by trying to get the secret variable
        _ = secretsmanager.get_secret_value(SecretId=full_secret_name)

    except secretsmanager.exceptions.ResourceNotFoundException:
        # A secret variable with that name does not exist, so create it
        if args.dry_run:
            # Create it for fakes
            print("Secret variable {} not found in secrets manager, dry-run creating it".format(short_secret_name))
        else:
            # Create it for real
            print("Secret variable {} not found in secrets manager, creating it".format(short_secret_name))
            _ = secretsmanager.create_secret(
                Name=short_secret_name, SecretString=secret_val
            )
    else:
        # Get operation was successful, secret variable exists
        if args.dry_run:
            # Update it for fakes
            print("Secret variable {} found in secrets manager, dry-run updating it".format(short_secret_name))
        else:
            # Update it for real
            print("Secret variable {} found in secrets manager, updating it".format(short_secret_name))
            _ = secretsmanager.update_secret(
                SecretId=short_secret_name, SecretString=secret_val
            )


@events.action("delete",
               arguments={
                   "--stage": dict(
                       required=False,
                       help="the stage for which secrets should be deleted"),
                   "--secret-name": dict(
                       required=True,
                       help="name of secret to delete (limit 1 at a time)"),
                   "--dry-run": dict(
                       default=False,
                       action="store_true",
                       help="do a dry run of the actual operation")})
def del_secret(argv: typing.List[str], args: argparse.Namespace):
    """
    Delete the value of the secret variable specified by the
    --secret-name flag from the secrets manager
    """
    store_name = os.environ['DSS_SECRETS_STORE']
    if args.stage is None:
        stage_name = os.environ['DSS_DEPLOYMENT_STAGE']

    # Make sure a secret name was specified
    if args.secret_name is None or len(args.secret_name) == 0:
        logger.error("Unable to set secret: no secret name was specified! Use the --secret-name flag.")
        sys.exit()
    secret_name = args.secret_name

    # Necessary because AWS requires full resource identifiers to delete secrets
    region_name = arn.get_region()
    account_id = arn.get_account_id()
    arn_prefix = f'arn:aws:secretsmanager:{region_name}:{account_id}:secret:'
    store_prefix = f'{store_name}/{stage_name}/'

    full_secret_name = get_long_name(secret_name, arn_prefix, store_prefix)
    short_secret_name = get_short_name(full_secret_name, arn_prefix, store_prefix)

    # Make sure the user really wants to do this
    confirm = """
    Are you really sure you want to delete secret {}? (Type 'y' or 'yes' to confirm):
    """
    response = input(confirm.format(secret_name))
    if response.lower() not in ['y', 'yes']:
        logger.error("You safely aborted the delete secret operation!")
        sys.exit()

    try:
        # Start by trying to get the secret variable
        _ = secretsmanager.get_secret_value(SecretId=full_secret_name)

    except secretsmanager.exceptions.ResourceNotFoundException:
        # No secret var found
        logger.warning("Secret variable {} not found in secrets manager!".format(short_secret_name))

    except secretsmanager.exceptions.InvalidRequestException:
        # Already deleted secret var
        logger.warning("Secret variable {} already marked for deletion in secrets manager!".format(short_secret_name))

    else:
        # Get operation was successful, secret variable exists
        if args.dry_run:
            # Delete it for fakes
            print("Secret variable {} found in secrets manager, dry-run deleting it".format(short_secret_name))
        else:
            # Delete it for real
            print("Secret variable {} found in secrets manager, deleting it".format(short_secret_name))
            _ = secretsmanager.delete_secret(SecretId=full_secret_name)
