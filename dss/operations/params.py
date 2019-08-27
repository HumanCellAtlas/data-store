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
import dss.util.aws.clients
from dss.operations.util import get_cloud_variable_prefix, set_cloud_env_var, unset_cloud_env_var, EmptyStdinException

lambda_client = getattr(dss.util.aws.clients, "lambda")


logger = logging.getLogger(__name__)


def set_ssm_var(env_var: str, value) -> None:
    return set_cloud_env_var(
        environment=get_ssm_environment(),
        set_fn=set_ssm_environment,
        env_var=env_var,
        value=value,
        where="in SSM",
    )


def set_lambda_var(env_var: str, value, lambda_name: str) -> None:
    return set_cloud_env_var(
        environment=get_deployed_lambda_environment(lambda_name),
        set_fn=set_deployed_lambda_environment,
        env_var=env_var,
        value=value,
        where=f"in lambda function {lambda_name}",
    )


def unset_ssm_var(env_var: str) -> None:
    return unset_cloud_env_var(
        environment=get_ssm_environment(),
        set_fn=set_ssm_environment,
        env_var=env_var,
        where="in SSM",
    )


def unset_lambda_var(env_var: str, lambda_name: str) -> None:
    return unset_cloud_env_var(
        environment=get_deployed_lambda_environment(lambda_name),
        set_fn=set_deployed_lambda_environment,
        env_var=env_var,
        where=f"in lambda function {lambda_name}",
    )


def get_ssm_environment() -> dict:
    prefix = get_cloud_variable_prefix()
    p = ssm_client.get_parameter(Name=f"/{prefix}/environment")
    parms = p["Parameter"]["Value"]
    # above value is a string; convert to dict
    return json.loads(parms)


def set_ssm_environment(parms: dict) -> None:
    prefix = get_cloud_variable_prefix()
    ssm_client.put_parameter(
        Name=f"/{prefix}/environment",
        Value=json.dumps(parms),
        Type="String",
        Overwrite=True,
    )


def get_deployed_lambda_environment(lambda_name: str) -> dict:
    c = lambda_client.get_function_configuration(FunctionName=lambda_name)
    # above value is a dict, no need to convert
    return c["Environment"]["Variables"]


def set_deployed_lambda_environment(lambda_name: str, env: dict) -> None:
    lambda_client.update_function_configuration(
        FunctionName=lambda_name, Environment={"Variables": env}
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


def print_lambda_env(lambda_name, lambda_env):
    print(f"\n{lambda_name}:")
    for name, val in lambda_env.items():
        print(f"{name}={val}")


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
    ssm_env = get_ssm_environment()
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
        set_ssm_var(name, val)


@params.action(
    "ssm-unset",
    arguments={
        "--name": dict(
            required=True,
            help="name of environment variable to unset in SSM param store",
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
        unset_ssm_var(name)


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

    # Iterate over each specified lambda and get its env vars
    d = {}
    for lambda_name in lambda_names:
        lambda_env = get_deployed_lambda_environment(lambda_name)
        d[lambda_name] = lambda_env

    # List each lambda's env vars
    if args.json:
        print(json.dumps(d, indent=4))
    else:
        for lambda_name, lambda_env in d.items:
            print_lambda_env(lambda_name, lambda_env)


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
        set_ssm_var(name, val)
        # Set the variable in each lambda function
        for lambda_name in get_deployed_lambdas():
            set_lambda_var(name, val, lambda_name)


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
        unset_ssm_var(name)

    # Unset the variable from each lambda function
    for lambda_name in get_deployed_lambdas():
        if args.dry_run:
            print(
                f'Dry-run deleting variable "{name}" from lambda function "{lambda_name}"'
            )
        else:
            unset_lambda_var(name, lambda_name)
