"""
Get and set parameters in the SSM parameter store "environment". Parameters are stored as key-value
pairs in a map named "environment" (stored in the SSM param store).
"""
import os
import sys
import select
import json
import argparse
import logging
import typing

from botocore.exceptions import ClientError

from dss.operations import dispatch
from dss.util.aws.clients import secretsmanager  # type: ignore
import dss.operations.util as util

from dss.util.aws.clients import ssm as ssm_client  # type: ignore
from dss.util.aws.clients import secretsmanager as sm_client  # type: ignore


logger = logging.getLogger(__name__)


def get_ssm_variable_prefix() -> str:
    """Use info from local environment to assemble necessary prefix for SSM param store variables"""
    store_name = os.environ["DSS_PARAMETER_STORE"]
    stage_name = os.environ["DSS_DEPLOYMENT_STAGE"]
    store_prefix = f"{store_name}/{stage_name}"
    return store_prefix


def fix_ssm_variable_prefix(param_name: str) -> str:
    """Add the variable store and stage prefix to the front of an SSM param name"""
    prefix = get_ssm_variable_prefix()
    if not (param_name.startswith(prefix) or param_name.startswith("/" + prefix)):
        param_name = f"{prefix}/{param_name}"
    return param_name


def get_ssm_environment() -> dict:
    """Get the value of the parameter named "environment" in the SSM param store"""
    prefix = get_ssm_variable_prefix()
    p = ssm_client.get_parameter(Name=f"/{prefix}/environment")
    parms = p["Parameter"]["Value"]  # this is a string, so convert to dict
    return json.loads(parms)


def set_ssm_environment(env: dict) -> None:
    """
    Set the SSM param store param "environment" to the values in env (dict).

    :param env: dict containing environment variables to set in SSM param store
    :returns: nothing
    """
    prefix = get_ssm_variable_prefix()
    ssm_client.put_parameter(
        Name=f"/{prefix}/environment", Value=json.dumps(env), Type="String", Overwrite=True
    )


def set_ssm_parameter(env_var: str, value) -> None:
    """
    Set a parameter in the SSM param store variable "environment".

    :param env_var: the name of the environment variable being set
    :param value: the value of the environment variable being set
    """
    environment = get_ssm_environment()
    try:
        prev_value = environment[env_var]
    except KeyError:
        prev_value = None
    environment[env_var] = value
    set_ssm_environment(environment)
    print(f"Success! Set variable in SSM parameter store environment:")
    print(f"Name: {env_var}")
    print(f"Value: {value}")
    if prev_value:
        print(f"Previous value: {prev_value}")


def unset_ssm_parameter(env_var: str) -> None:
    """
    Unset a parameter in the SSM param store variable "environment".

    :param env_var: the name of the environment variable being set
    """
    environment = get_ssm_environment()
    try:
        prev_value = environment[env_var]
        del environment[env_var]
        set_ssm_environment(environment)
        print(f"Success! Unset variable in SSM parameter store environment:")
        print(f"Name: {env_var} ")
        print(f"Previous value: {prev_value}")
    except KeyError:
        print(f"Nothing to unset for variable {env_var} in SSM parameter store environment")


ssm_params = dispatch.target("params", arguments={}, help=__doc__)


@ssm_params.action(
    "list",
    arguments={
        "--json": dict(default=False, action="store_true", help="format the output as JSON")
    },
)
def ssm_list(argv: typing.List[str], args: argparse.Namespace):
    """Print out all variables stored in the SSM store"""
    ssm_env = get_ssm_environment()
    if args.json:
        print(json.dumps(ssm_env, indent=4))
    else:
        for name, val in ssm_env.items():
            print(f"{name}={val}")
        print("\n")


@ssm_params.action(
    "set",
    arguments={
        "name": dict(help="name of variable to set in SSM param store environment"),
        "--dry-run": dict(
            default=False, action="store_true", help="do a dry run of the actual operation"
        ),
    },
)
def ssm_set(argv: typing.List[str], args: argparse.Namespace):
    """Set a variable in the SSM param store environment"""
    name = args.name

    # Use stdin (input piped to script)
    if not select.select([sys.stdin], [], [], 0.0)[0]:
        raise RuntimeError("Error: stdin was empty! A variable value must be provided via stdin")
    val = sys.stdin.read()

    if args.dry_run:
        print(f"Dry-run creating variable in SSM param store environment:")
        print(f"Name: {name}")
        print(f"Value: {val}")
    else:
        set_ssm_parameter(name, val)


@ssm_params.action(
    "unset",
    arguments={
        "name": dict(help="name of variable to unset in SSM param store environment"),
        "--dry-run": dict(
            default=False, action="store_true", help="do a dry run of the actual operation"
        ),
    },
)
def ssm_unset(argv: typing.List[str], args: argparse.Namespace):
    name = args.name

    # Unset the variable from the SSM store first
    if args.dry_run:
        print(f'Dry-run deleting variable "{name}" from SSM store')
    else:
        unset_ssm_parameter(name)
