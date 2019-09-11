"""
Get/set secret variable values from the AWS Secrets Manager
"""
import os
import sys
import select
import typing
import argparse
import json
import logging
from botocore.exceptions import ClientError

from dss.operations import dispatch
from dss.operations.util import EmptyStdinException
from dss.util.aws.clients import secretsmanager as sm_client  # type: ignore
import dss.operations.util as util


logger = logging.getLogger(__name__)


secrets = dispatch.target("secrets", arguments={}, help=__doc__)


def secret_is_gettable(secret_name):
    """Secrets are gettable if they exist in the secrets manager"""
    # Secrets can be in three different states:
    # - secret exists (gettable, settable)
    # - secret exists but is marked for deletion (not gettable, not settable)
    # - secret does not exist (not gettable, settable)
    try:
        sm_client.get_secret_value(SecretId=secret_name)
    except ClientError:
        return False
    else:
        return True


def secret_is_settable(secret_name):
    """Secrets are settable if they exist in the secrets manager or if they are not found"""
    try:
        sm_client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        if 'Error' in e.response:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                return True
        return False
    else:
        return True


def get_secret_variable_prefix() -> str:
    """Use information from the environment to assemble the necessary prefix for secret variables."""
    store_name = os.environ["DSS_SECRETS_STORE"]
    stage_name = os.environ["DSS_DEPLOYMENT_STAGE"]
    store_prefix = f"{store_name}/{stage_name}"
    return store_prefix


def fix_secret_variable_prefix(secret_name: str) -> str:
    """This adds the variable store and stage prefix to the front of a secret variable name"""
    prefix = get_secret_variable_prefix()
    if not secret_name.startswith(prefix):
        secret_name = f"{prefix}/{secret_name}"
    return secret_name


@secrets.action(
    "list",
    arguments={
        "--json": dict(
            default=False,
            action="store_true",
            help="format the output as JSON if this flag is present",
        )
    },
)
def list_secrets(argv: typing.List[str], args: argparse.Namespace):
    """
    Print a list of names of every secret variable in the secrets manager
    for the given store and stage.
    """
    store_prefix = get_secret_variable_prefix()

    paginator = sm_client.get_paginator("list_secrets")

    secret_names = []
    for response in paginator.paginate():
        for secret in response["SecretList"]:

            # Get resource IDs
            secret_name = secret["Name"]

            # Only save secrets for this store and stage
            if secret_name.startswith(store_prefix):
                secret_names.append(secret_name)

    secret_names.sort()

    if args.json is True:
        print(json.dumps(secret_names, indent=4))
    else:
        for secret_name in secret_names:
            print(secret_name)


@secrets.action(
    "get",
    arguments={
        "--secret-names": dict(
            required=True,
            nargs="*",
            help="names of secrets to retrieve (separate multiple values with spaces)",
        ),
        "--json": dict(
            default=False,
            action="store_true",
            help="format the output as JSON if this flag is present",
        ),
    },
)
def get_secret(argv: typing.List[str], args: argparse.Namespace):
    """
    Get the value of the secret variable (or list of variables) specified by
    the --secret-names flag; separate multiple secret names using a space.
    """
    # Note: this function should not print anything except the final JSON,
    # in case the user pipes the JSON output of this script to something else
    secret_names = args.secret_names

    # Tack on the store prefix if it isn't there already
    secret_names = [fix_secret_variable_prefix(j) for j in secret_names]

    # Determine if we should format output as JSON
    use_json = False
    if args.json is True:
        use_json = True

    for secret_name in secret_names:
        if secret_is_gettable(secret_name):
            response = sm_client.get_secret_value(SecretId=secret_name)
            secret_val = response["SecretString"]
            # Sometimes secret_val is a dictionary
            try:
                secret_val = json.loads(secret_val)
            except json.decoder.JSONDecodeError:
                # and sometimes it isn't
                pass
            # Print
            if use_json:
                print(json.dumps({secret_name: secret_val}, indent=4))
            else:
                print(f"{secret_name}={secret_val}")


@secrets.action(
    "set",
    arguments={
        "--secret-name": dict(
            required=True, help="name of secret to set (limit 1 at a time)"
        ),
        "--secret-value": dict(
            required=False,
            default=None,
            help="value of secret to set (optional, if not present then stdin will be used)",
        ),
        "--dry-run": dict(
            default=False,
            action="store_true",
            help="do a dry run of the actual operation",
        ),
    },
)
def set_secret(argv: typing.List[str], args: argparse.Namespace):
    """Set the value of the secret variable specified by the --secret-name flag"""
    secret_name = args.secret_name

    # Tack on the store prefix if it isn't there already
    secret_name = fix_secret_variable_prefix(secret_name)

    # Decide what to use for input
    if args.secret_value is not None:
        secret_val = args.secret_value
    else:
        # Use stdin (input piped to this script) as secret value.
        # stdin provides secret value, flag --secret-name provides secret name.
        if not select.select([sys.stdin], [], [])[0]:
            raise EmptyStdinException()
        secret_val = sys.stdin.read()

    if secret_is_settable(secret_name):
        if args.dry_run:
            # Update it for fakes
            print(f"Dry-run updating secret variable {secret_name} in secrets manager")
        else:
            # Update it for real
            sm_client.update_secret(SecretId=secret_name, SecretString=secret_val)
            print(f"Updated secret variable {secret_name} in secrets manager")


@secrets.action(
    "delete",
    arguments={
        "--secret-name": dict(
            required=True, help="name of secret to delete (limit 1 at a time)"
        ),
        "--force": dict(
            default=False,
            action="store_true",
            help="force the delete operation to happen non-interactively (no user prompt)",
        ),
        "--dry-run": dict(
            default=False,
            action="store_true",
            help="do a dry run of the actual operation",
        ),
    },
)
def del_secret(argv: typing.List[str], args: argparse.Namespace):
    """
    Delete the value of the secret variable specified by the
    --secret-name flag from the secrets manager
    """
    secret_name = args.secret_name

    # Tack on the store prefix if it isn't there already
    secret_name = fix_secret_variable_prefix(secret_name)

    if args.force is False:
        # Make sure the user really wants to do this
        confirm = f"""
        Are you really sure you want to delete secret {secret_name}? (Type 'y' or 'yes' to confirm):
        """
        response = input(confirm)
        if response.lower() not in ["y", "yes"]:
            raise RuntimeError("You safely aborted the delete secret operation!")

    if secret_is_settable(secret_name):
        if args.dry_run:
            # Delete it for fakes
            print(f"Dry-run deleting secret variable {secret_name} in secrets manager")
        else:
            # Delete it for real
            sm_client.delete_secret(SecretId=secret_name)
            print(f"Deleted secret variable {secret_name} in secrets manager")
