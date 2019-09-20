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
from dss.util.aws.clients import secretsmanager  # type: ignore

logger = logging.getLogger(__name__)


def get_secret_store_prefix() -> str:
    """
    Use information from the environment to assemble the necessary prefix for accessing variables in the
    SecretsManager.
    """
    store_name = os.environ["DSS_SECRETS_STORE"]
    stage_name = os.environ["DSS_DEPLOYMENT_STAGE"]
    store_prefix = f"{store_name}/{stage_name}"
    return store_prefix


def fix_secret_variable_prefix(secret_name: str) -> str:
    """
    Given a secret name, check if it already has the secrets store prefix.
    """
    prefix = get_secret_store_prefix()
    if not (secret_name.startswith(prefix) or secret_name.startswith("/" + prefix)):
        secret_name = f"{prefix}/{secret_name}"
    return secret_name


def fetch_secret_safely(secret_name: str) -> dict:
    """
    Fetch a secret from the store safely, raising errors if the secret is not found or is marked for deletion.
    """
    try:
        response = sm_client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        if "Error" in e.response:
            errtype = e.response["Error"]["Code"]
            if errtype == "ResourceNotFoundException":
                raise RuntimeError(f"Error: secret {secret_name} was not found!")
        raise RuntimeError(f"Error: could not fetch secret {secret_name} from secrets manager")
    else:
        return response


events = dispatch.target("secrets", arguments={}, help=__doc__)


json_flag_options = dict(
    default=False, action="store_true", help="format the output as JSON if this flag is present"
)
dryrun_flag_options = dict(default=False, action="store_true", help="do a dry run of the actual operation")


@events.action(
    "list",
    arguments={
        "--json": dict(
            default=False,
            action="store_true",
            help="format the output as a JSON list if this flag is present",
        )
    },
)
def list_secrets(argv: typing.List[str], args: argparse.Namespace):
    """
    Print a list of names of every secret variable in the secrets manager for the DSS secrets manager
    for $DSS_DEPLOYMENT_STAGE.
    """
    store_prefix = get_secret_store_prefix()

    paginator = secretsmanager.get_paginator("list_secrets")

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


@events.action(
    "get",
    arguments={
        "secret_name": dict(help="the name of the secret to retrieve (separate multiple values with spaces)"),
        "--outfile": dict(required=False, type=str, help="specify an output file where the secret will be saved"),
        "--force": dict(
            default=False,
            action="store_true",
            help="overwrite the output file, if it already exists; must be used with --output flag.",
        ),
    },
)
def get_secret(argv: typing.List[str], args: argparse.Namespace):
    """
    Get the value of the secret variable specified by secret_name.
    """
    # Note: this function should not print anything except the final JSON, in case the user pipes the JSON
    # output of this script to something else

    store_prefix = get_secret_store_prefix()

    if args.outfile:
        if os.path.exists(args.outfile) and not args.force:
            raise RuntimeError(
                f"Error: file {args.outfile} already exists, use the --force flag to overwrite it"
            )

    secret_name = fix_secret_variable_prefix(args.secret_name)

    # Attempt to obtain secret
    try:
        response = secretsmanager.get_secret_value(SecretId=secret_name)
        secret_val = response["SecretString"]
    except ClientError:
        # A secret variable with that name does not exist
        print(f"Error: Resource not found: {secret_name}")
    else:
        # Get operation was successful, secret variable exists
        if args.outfile:
            sys.stdout = open(args.outfile, "w")
        print(secret_val)
        if args.outfile:
            sys.stdout = sys.__stdout__


@events.action(
    "set",
    arguments={
        "secret_name": dict(help="name of secret to set (limit 1 at a time)"),
        "--dry-run": dict(default=False, action="store_true", help="do a dry run of the actual operation"),
        "--infile": dict(help="specify an input file whose contents is the secret value"),
        "--force": dict(
            default=False, action="store_true", help="force the action to happen (no interactive prompt)"
        ),
    },
)
def set_secret(argv: typing.List[str], args: argparse.Namespace):
    """Set the value of the secret variable."""
    store_prefix = get_secret_store_prefix()

    secret_name = fix_secret_variable_prefix(args.secret_name)

    # Decide what to use for infile
    secret_val = None
    if args.infile is not None:
        if os.path.isfile(args.infile):
            with open(args.infile, 'r') as f:
                secret_val = f.read()
        else:
            raise RuntimeError(f"Error: specified input file {args.infile} does not exist!")
    else:
        # Use stdin (input piped to this script) as secret value.
        # stdin provides secret value, flag --secret-name provides secret name.
        if not select.select([sys.stdin], [], [], 0.0)[0]:
            raise RuntimeError("Error: stdin was empty! A secret value must be provided via stdin")
        secret_val = sys.stdin.read()

    # Create or update
    try:
        # Start by trying to get the secret variable
        secretsmanager.get_secret_value(SecretId=secret_name)

    except ClientError:
        # A secret variable with that name does not exist, so create it
        if args.dry_run:
            print(f"Secret variable {secret_name} not found in secrets manager, dry-run creating it")
        else:
            if args.infile:
                print(f"Secret variable {secret_name} not found in secrets manager, creating from input file")
            else:
                print(f"Secret variable {secret_name} not found in secrets manager, creating from stdin")
            secretsmanager.create_secret(Name=secret_name, SecretString=secret_val)

    else:
        # Get operation was successful, secret variable exists
        # Prompt the user before overwriting, unless --force flag present
        if not args.force and not args.dry_run:
            # Prompt the user to make sure they really want to do this
            confirm = f"""
            *** WARNING!!! ***

            The secret you are setting currently has a value. Calling the
            set secret function will overwrite the current value of the
            secret!

            Note:
            - To do a dry run of this operation first, use the --dry-run flag.
            - To ignore this warning, use the --force flag.

            Are you really sure you want to update the secret?
            (Type 'y' or 'yes' to confirm):
            """
            response = input(confirm)
            if response.lower() not in ["y", "yes"]:
                raise RuntimeError("You safely aborted the set secret operation!")

        if args.dry_run:
            print(f"Secret variable {secret_name} found in secrets manager, dry-run updating it")
        else:
            if args.infile:
                print(f"Secret variable {secret_name} not found in secrets manager, updating from input file")
            else:
                print(f"Secret variable {secret_name} not found in secrets manager, updating from stdin")
            secretsmanager.update_secret(SecretId=secret_name, SecretString=secret_val)


@events.action(
    "delete",
    arguments={
        "secret_name": dict(help="name of secret to delete (limit 1 at a time)"),
        "--force": dict(
            default=False,
            action="store_true",
            help="force the delete operation to happen non-interactively (no user prompt)",
        ),
        "--dry-run": dict(default=False, action="store_true", help="do a dry run of the actual operation"),
    },
)
def del_secret(argv: typing.List[str], args: argparse.Namespace):
    """
    Delete the value of the secret variable specified by the
    --secret-name flag from the secrets manager
    """
    store_prefix = get_secret_store_prefix()

    secret_name = fix_secret_variable_prefix(args.secret_name)

    try:
        # Start by trying to get the secret variable
        secretsmanager.get_secret_value(SecretId=secret_name)

    except ClientError:
        # No secret var found
        logger.warning(f"Secret variable {secret_name} not found in secrets manager!")

    except secretsmanager.exceptions.InvalidRequestException:
        # Already deleted secret var
        logger.warning(f"Secret variable {secret_name} already marked for deletion in secrets manager!")

    else:
        # Get operation was successful, secret variable exists
        if not args.force and not args.dry_run:
            # Make sure the user really wants to do this
            confirm = f"""
            *** WARNING!!! ****

            You are about to delete secret {secret_name} from the secrets
            manager. Are you sure you want to delete the secret?
            (Type 'y' or 'yes' to confirm):
            """
            response = input(confirm)
            if response.lower() not in ["y", "yes"]:
                raise RuntimeError("You safely aborted the delete secret operation!")

        if args.dry_run:
            # Delete it for fakes
            print(f"Secret variable {secret_name} found in secrets manager, dry-run deleting it")
        else:
            # Delete it for real
            print(f"Secret variable {secret_name} found in secrets manager, deleting it")
            secretsmanager.delete_secret(SecretId=secret_name)
