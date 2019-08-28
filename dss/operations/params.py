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
from dss.operations.util import EmptyStdinException
import dss.operations.util as util
from dss.util.aws.clients import ssm as ssm_client  # type: ignore
from dss.util.aws.clients import secretsmanager as sm_client  # type: ignore
import dss.util.aws.clients

lambda_client = getattr(dss.util.aws.clients, "lambda")


logger = logging.getLogger(__name__)


def set_ssm_var(env_var: str, value) -> None:
    """Add a single variable to the SSM parameter store"""
    return util.set_cloud_env_var(
        environment=get_ssm_environment(),
        env_set_fn=set_ssm_environment,
        env_var=env_var,
        value=value,
        where="in SSM",
    )


def set_lambda_var(env_var: str, value, lambda_name: str) -> None:
    """Set a single variable in the environment of the specified lambda function"""
    return util.set_cloud_env_var(
        environment=get_deployed_lambda_environment(lambda_name),
        env_set_fn=lambda env: set_deployed_lambda_environment(lambda_name, env),
        env_var=env_var,
        value=value,
        where=f"in lambda function {lambda_name}",
    )


def unset_ssm_var(env_var: str) -> None:
    """Unset a single variable in the SSM parameter store"""
    return util.unset_cloud_env_var(
        environment=get_ssm_environment(),
        env_set_fn=set_ssm_environment,
        env_var=env_var,
        where="in SSM",
    )


def unset_lambda_var(env_var: str, lambda_name: str) -> None:
    """Unset a single variable in the environment of the specified lambda function"""
    return util.unset_cloud_env_var(
        environment=get_deployed_lambda_environment(lambda_name),
        env_set_fn=lambda env: set_deployed_lambda_environment(lambda_name, env),
        env_var=env_var,
        where=f"in lambda function {lambda_name}",
    )


def get_ssm_environment() -> dict:
    """Get the value of the parameter 'environment' in the SSM parameter store"""
    prefix = util.get_cloud_variable_prefix()
    p = ssm_client.get_parameter(Name=f"/{prefix}/environment")
    parms = p["Parameter"]["Value"]
    # above value is a string; convert to dict
    return json.loads(parms)


def set_ssm_environment(parms: dict) -> None:
    """Set the value of the parameter 'environment' in the SSM parameter store"""
    prefix = util.get_cloud_variable_prefix()
    ssm_client.put_parameter(
        Name=f"/{prefix}/environment",
        Value=json.dumps(parms),
        Type="String",
        Overwrite=True,
    )


def get_deployed_lambdas():
    """Generator returning names of lambda functions"""
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


def get_deployed_lambda_environment(lambda_name: str) -> dict:
    """Get the environment variables of a deployed lambda function"""
    c = lambda_client.get_function_configuration(FunctionName=lambda_name)
    # above value is a dict, no need to convert
    return c["Environment"]["Variables"]


def set_deployed_lambda_environment(lambda_name: str, env: dict) -> None:
    """Set the environment variables of a deployed lambda function"""
    lambda_client.update_function_configuration(
        FunctionName=lambda_name, Environment={"Variables": env}
    )


def get_local_lambda_environment() -> dict:
    """Get the local value of all environment variables set in lambda functions"""
    env = dict()
    for name in os.environ["EXPORT_ENV_VARS_TO_LAMBDA"].split():
        try:
            env[name] = os.environ[name]
        except KeyError:
            logger.warning(
                f"Warning: environment variable {name} is in the list of environment variables "
                f"to export to lambda environment, EXPORT_ENV_VARS_TO_LAMBDA, but variable is "
                f"not defined in the local environment, so there is no value to set."
            )
    return env


def _print_lambda_env(lambda_name, lambda_env):
    """Print the environment variables set in a specified lambda function"""
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
        for lambda_name, lambda_env in d.items():
            _print_lambda_env(lambda_name, lambda_env)


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


@params.action(
    "lambda-update",
    arguments={
        "--update-deployed": dict(
            default=False,
            action="store_true",
            help="update the environment variables of all deployed lambdas, "
            "in addition to updating the lambda environment stored in "
            "the SSM store",
        ),
        "--dry-run": dict(
            default=False,
            action="store_true",
            help="do a dry run of the actual operation",
        ),
    },
)
def lambda_update(argv: typing.List[str], args: argparse.Namespace):
    """
    Update the lambda environment stored in the SSM store
    (and optionally update the environment of each deployed lambda).
    """
    # Update lambda environment stored in SSM store
    local_env = get_local_lambda_environment()
    local_env["DSS_ES_ENDPOINT"] = util.get_elasticsearch_endpoint()
    local_env["ADMIN_USER_EMAILS"] = util.get_admin_emails()
    if args.dry_run:
        print(f"Dry-run resetting lambda environment stored in SSM parameter store")
    else:
        set_ssm_environment(local_env)
        print(f"Finished resetting lambda environment stored in SSM parameter store")

    # Update environment of each deployed lambda
    if args.update_deployed:
        for lambda_name in get_deployed_lambdas():
            lambda_env = get_deployed_lambda_environment(lambda_name)
            lambda_env.update(local_env)
            if args.dry_run:
                print(
                    f"Dry-run resetting lambda environment for lambda function {lambda_name} "
                    f"using new lambda environment in SSM parameter store"
                )
            else:
                set_deployed_lambda_environment(lambda_name, lambda_env)
                print(
                    f"Finished resetting lambda environment for lambda function {lambda_name} "
                    f"using new lambda environment in SSM parameter store"
                )
