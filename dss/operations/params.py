"""
Get, set, and unset environment variables in the SSM store and in deployed lambda functions
"""
import os
import sys
import typing
import argparse
import json
import logging
import select

from dss.operations import dispatch
from dss.util.aws.clients import ssm as ssm_client  # type: ignore
from dss.util.aws.clients import es as es_client  # type: ignore
from dss.util.aws.clients import secretsmanager as sm_client
import dss.util.aws.clients
from dss.operations.util import get_variable_prefix, EmptyStdinException

lambda_client = getattr(dss.util.aws.clients, "lambda")


logger = logging.getLogger(__name__)


def get_ssm_lambda_environment() -> dict:
    prefix = get_variable_prefix()
    p = ssm_client.get_parameter(Name=f"/{prefix}/environment")
    parms = p["Parameter"]["Value"]
    # above value is a string; convert to dict
    return json.loads(parms)


def set_ssm_lambda_environment(parms: dict):
    prefix = get_variable_prefix()
    ssm_client.put_parameter(
        Name=f"/{prefix}/environment",
        Value=json.dumps(parms),
        Type="String",
        Overwrite=True,
    )


def get_deployed_lambda_environment(name: str) -> dict:
    c = lambda_client.get_function_configuration(FunctionName=name)
    # above value is a dict, no need to convert
    return c["Environment"]["Variables"]


def set_deployed_lambda_environment(name: str, env: dict):
    lambda_client.update_function_configuration(
        FunctionName=name, Environment={"Variables": env}
    )


def get_deployed_lambdas():
    _, dirs, _ = next(os.walk(os.path.join(os.environ["DSS_HOME"], "daemons")))
    stage = os.environ["DSS_DEPLOYMENT_STAGE"]
    functions = [f"{name}-{stage}" for name in dirs]
    functions.append(f"dss-{stage}")
    for name in functions:
        try:
            lambda_client.get_function(FunctionName=name)
            yield name
        except lambda_client.exceptions.ResourceNotFoundException:
            logger.warning(f"{name} not deployed, or does not deploy a Lambda function")


params = dispatch.target("params", arguments={}, help=__doc__)


@params.action(
    "ssm-list",
    arguments={
        "--json": dict(
            default=False,
            action="store_true",
            help="format the output as JSON if this flag is present",
        )
    },
)
def ssm_list(argv: typing.List[str], args: argparse.Namespace):
    """Print out all environment variables stored in the SSM store"""
    # Iterate over all env vars and print them out
    ssm_env = get_ssm_lambda_environment()
    if args.json:
        print(json.dumps(ssm_env, indent=4))
    else:
        for name, val in ssm_env.items():
            print(f"{name}={val}")
        print("\n")


@params.action(
    "ssm-set",
    arguments={
        "--name": dict(
            required=True, help="name of environment variable to set in SSM param store"
        ),
        "--value": dict(
            required=False,
            default=None,
            help="value of environment variable "
            "(optional, if not present then stdin "
            "will be used)",
        ),
        "--dry-run": dict(
            default=False,
            action="store_true",
            help="do a dry run of the actual operation",
        ),
    },
)
def ssm_set(argv: typing.List[str], args: argparse.Namespace):
    """Set an environment variable in the SSM store"""
    name = args.name

    # Decide what to do for input
    if args.value is not None:
        # Use --value
        val = args.value
    else:
        # Use stdin (input piped to script)
        if not select.select([sys.stdin], [], [])[0]:
            raise EmptyStdinException()
        val = sys.stdin.read()

    if args.dry_run:
        print(f'Dry-run creating variable "{name}" with value "{val}" in SSM store')
    else:
        # Set the variable in the SSM store
        ssm_env = get_ssm_lambda_environment()
        ssm_env[name] = val
        set_ssm_lambda_environment(ssm_env)
        print(f'Created variable "{name}" with value "{val}" in SSM store')


@params.action(
    "ssm-unset",
    arguments={
        "--name": dict(
            required=True, help="name of environment variable to unset in SSM param store"
        ),
        "--dry-run": dict(
            default=False,
            action="store_true",
            help="do a dry run of the actual operation",
        ),
    },
)
def ssm_unset(argv: typing.List[str], args: argparse.Namespace):
    """Unset an environment variable in the SSM store"""
    name = args.name

    # Unset the variable from the SSM store first
    if args.dry_run:
        print(f'Dry-run deleting variable "{name}" from SSM store')
    else:
        ssm_env = get_ssm_lambda_environment()
        try:
            del ssm_env[name]
        except KeyError:
            pass
        set_ssm_lambda_environment(ssm_env)
        print(f'Deleted variable "{name}" from SSM store')


@params.action(
    "lambda-list",
    arguments={
        "--json": dict(
            default=False,
            action="store_true",
            help="format the output as JSON if this flag is present",
        ),
        "--lambda-name": dict(
            required=False,
            default=None,
            help="specify the name of a lambda function whose environment will be listed",
        ),
    },
)
def lambda_list(argv: typing.List[str], args: argparse.Namespace):
    """Print out the current environment of all deployed lambda functions"""
    # Determine if we are doing this for all lambdas
    # or one specific lambda
    if args.lambda_name:
        lambda_names = [args.lambda_name]
    else:
        lambda_names = get_deployed_lambdas()

    # Iterate over each specified lambda,
    # get its current environment,
    # and list all its env vars
    if args.json:
        # Need to assemble our own dictionary
        d = {}
        for lambda_name in lambda_names:
            lambda_env = get_deployed_lambda_environment(lambda_name)
            d[lambda_name] = lambda_env
        print(json.dumps(d, indent=4))

    else:
        # Iterate over each specified lambda function
        # and print its environment
        for lambda_name in lambda_names:
            lambda_env = get_deployed_lambda_environment(lambda_name)
            print(f"\n{lambda_name}:")
            for name, val in lambda_env.items():
                print(f"{name}={val}")


@params.action(
    "lambda-set",
    arguments={
        "--name": dict(
            required=True,
            help="name of environment variable to set in all lambda environments",
        ),
        "--value": dict(
            required=False,
            default=None,
            help="value of environment variable "
            "(optional, if not present then stdin "
            "will be used)",
        ),
        "--dry-run": dict(
            default=False,
            action="store_true",
            help="do a dry run of the actual operation",
        ),
    },
)
def lambda_set(argv: typing.List[str], args: argparse.Namespace):
    """Set an environment variable in each deployed lambda"""
    name = args.name

    # Decide what to do for input
    if args.value is not None:
        # Use --value
        val = args.value
    else:
        # Use stdin (input piped to script)
        if not select.select([sys.stdin], [], [])[0]:
            raise EmptyStdinException()
        val = sys.stdin.read()

    if args.dry_run:
        print(f"Dry-run creating variable {name} in SSM store")
        for lambda_name in get_deployed_lambdas():
            print(f"Dry-run creating variable {name} in lambda {lambda_name}")
    else:
        # Set the variable in the SSM store first
        ssm_env = get_ssm_lambda_environment()
        ssm_env[name] = val
        set_ssm_lambda_environment(ssm_env)
        print(f"Created variable {name} in SSM store")
        # Set the variable in each lambda function
        for lambda_name in get_deployed_lambdas():
            lambda_env = get_deployed_lambda_environment(lambda_name)
            lambda_env[name] = val
            set_deployed_lambda_environment(lambda_name, lambda_env)
            print(f"Created variable {name} in lambda {lambda_name}")


@params.action(
    "lambda-unset",
    arguments={
        "--name": dict(
            required=True,
            help="name of environment variable to unset in all lambda environments",
        ),
        "--dry-run": dict(
            default=False,
            action="store_true",
            help="do a dry run of the actual operation",
        ),
    },
)
def lambda_unset(argv: typing.List[str], args: argparse.Namespace):
    """Unset an environment variable in each deployed lambda"""
    name = args.name

    # Unset the variable from the SSM store first
    if args.dry_run:
        print(f'Dry-run deleting variable "{name}" from SSM store')
    else:
        ssm_env = get_ssm_lambda_environment()
        try:
            del ssm_env[name]
        except KeyError:
            pass
        set_ssm_lambda_environment(ssm_env)
        print(f'Deleted variable "{name}" from SSM store')

    # Unset the variable from each lambda function
    for lambda_name in get_deployed_lambdas():
        if args.dry_run:
            print(f'Dry-run deleting variable "{name}" from lambda function "{lambda_name}"')
        else:
            lambda_env = get_deployed_lambda_environment(lambda_name)
            try:
                del lambda_env[name]
            except KeyError:
                pass
            set_deployed_lambda_environment(lambda_name, lambda_env)
            print(f'Deleted variable "{name}" from lambda function "{lambda_name}"')
