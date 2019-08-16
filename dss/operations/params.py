"""
Get, set, and unset environment variables in the SSM store and in deployed lambda functions
"""
import boto3
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

lambda_client = getattr(dss.util.aws.clients, "lambda")


logger = logging.getLogger(__name__)


def get_ssm_prefix():
    """
    Use information from the environment to assemble
    the necessary prefix for accessing variables in
    the parameter store.
    """
    store_name = os.environ["DSS_SECRETS_STORE"]
    stage_name = os.environ["DSS_DEPLOYMENT_STAGE"]
    store_prefix = f"{store_name}/{stage_name}"
    return store_prefix


def get_ssm_lambda_environment(prefix):
    p = ssm_client.get_parameter(Name=f"/{prefix}/environment")
    parms = p["Parameter"]["Value"]
    return json.loads(parms)


def set_ssm_lambda_environment(parms: dict):
    store = os.environ["DSS_PARAMETER_STORE"]
    stage = os.environ["DSS_DEPLOYMENT_STAGE"]
    ssm_client.put_parameter(
        Name=f"/{store}/{stage}/environment",
        Value=json.dumps(parms),
        Type="String",
        Overwrite=True,
    )


def get_deployed_lambda_environment(name):
    c = lambda_client.get_function_configuration(FunctionName=name)
    return c["Environment"]["Variables"]


def set_deployed_lambda_environment(name, env: dict):
    lambda_client.update_function_configuration(
        FunctionName=name, Environment={"Variables": env}
    )


def get_deployed_lambdas():
    root, dirs, files = next(os.walk(os.path.join(os.environ["DSS_HOME"], "daemons")))
    stage = os.environ["DSS_DEPLOYMENT_STAGE"]
    functions = [f"{name}-{stage}" for name in dirs]
    functions.append(f"dss-{stage}")
    for name in functions:
        try:
            lambda_client.get_function(FunctionName=name)
            yield name
        except lambda_client.exceptions.ResourceNotFoundException:
            print(f"{name} not deployed, or does not deploy a Lambda function")


def get_elasticsearch_endpoint():
    domain_name = os.environ["DSS_ES_DOMAIN"]
    domain_info = es_client.describe_elasticsearch_domain(DomainName=domain_name)
    return domain_info["DomainStatus"]["Endpoint"]


def get_admin_user_emails():
    store = os.environ["DSS_SECRETS_STORE"]
    stage = os.environ["DSS_DEPLOYMENT_STAGE"]
    secret_base = f"{store}/{stage}/"

    g_secrets_name = os.environ["GOOGLE_APPLICATION_CREDENTIALS_SECRETS_NAME"]
    gcp_secret_id = secret_base + g_secrets_name
    admin_secret_id = secret_base + os.environ["ADMIN_USER_EMAILS_SECRETS_NAME"]
    resp = boto3.client("secretsmanager").get_secret_value(SecretId=gcp_secret_id)
    gcp_service_account_email = json.loads(resp["SecretString"])["client_email"]
    resp = boto3.client("secretsmanager").get_secret_value(SecretId=admin_secret_id)
    admin_user_emails = [
        email for email in resp["SecretString"].split(",") if email.strip()
    ]
    admin_user_emails.append(gcp_service_account_email)
    return ",".join(admin_user_emails)


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
    prefix = get_ssm_prefix()

    # Iterate over all env vars and print them out
    ssm_env = get_ssm_lambda_environment(prefix)
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
            required=True, help="name of variable to set " "in the environment"
        ),
        "--value": dict(
            required=False,
            default=None,
            help="value to set for environment variable "
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
    """Set new environment variable(s) in the SSM store"""
    prefix = get_ssm_prefix()

    # Ensure variable name specified
    if len(args.name) == 0:
        msg = "Unable to set variable: no variable name provided. "
        msg += "Use the --name flag to specify variable name."
        raise RuntimeError(msg)
    name = args.name

    # Decide what to do for input
    if args.value is not None:
        # Use --value
        val = args.value
    else:
        # Use stdin (input piped to script)
        if not select.select([sys.stdin], [], [])[0]:
            err_msg = f"No data in stdin, cannot set variable {name} "
            err_msg += "without a value from stdin or specified with "
            err_msg += "--value flag!"
            raise RuntimeError(err_msg)
        val = sys.stdin.read()

    if args.dry_run:
        print(f"Dry-run creating variable \"{name}\" with value \"{val}\" in SSM store")
    else:
        # Set the variable in the SSM store
        ssm_env = get_ssm_lambda_environment(prefix)
        ssm_env[name] = val
        set_ssm_lambda_environment(ssm_env)


@params.action(
    "lambda-list",
    arguments={
        "--json": dict(
            default=False,
            action="store_true",
            help="format the output as JSON if this flag is present",
        )
    },
)
def lambda_list(argv: typing.List[str], args: argparse.Namespace):
    """Print out the current environment of all deployed lambda functions"""
    # Iterate over each deployed lambda,
    # get its current environment,
    # and list all env vars
    if args.json:
        # Need to assemble our own dictionary
        d = {}
        for lambda_name in get_deployed_lambdas():
            lambda_env = get_deployed_lambda_environment(lambda_name)
            d[lambda_name] = lambda_env
        print(json.dumps(d, indent=4))
    else:
        for lambda_name in get_deployed_lambdas():
            lambda_env = get_deployed_lambda_environment(lambda_name)
            print(f"\n{lambda_name}:")
            for name, val in lambda_env.items():
                print(f"{name}={val}")


@params.action(
    "lambda-set",
    arguments={
        "--name": dict(
            required=True, help="name of variable to set " "in the environment"
        ),
        "--value": dict(
            required=False,
            default=None,
            help="value to set for environment variable "
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
    """Set a single environment variable in each deployed lambda"""
    prefix = get_ssm_prefix()

    # Ensure variable name specified
    if len(args.name) == 0:
        msg = "Unable to set variable: no variable name provided. "
        msg += "Use the --name flag to specify variable name."
        raise RuntimeError(msg)
    name = args.name

    # Decide what to do for input
    if args.value is not None:
        # Use --value
        val = args.value
    else:
        # Use stdin (input piped to script)
        if not select.select([sys.stdin], [], [])[0]:
            err_msg = f"No data in stdin, cannot set variable {name} "
            err_msg += "without a value from stdin or specified with "
            err_msg += "--value flag!"
            raise RuntimeError(err_msg)
        val = sys.stdin.read()

    if args.dry_run:
        print(f"Dry-run creating variable {name} in SSM store")
        for lambda_name in get_deployed_lambdas():
            print(f"Dry-run creating variable {name} in lambda {lambda_name}")
    else:
        # Set the variable in the SSM store first
        ssm_env = get_ssm_lambda_environment(prefix)
        ssm_env[name] = val
        set_ssm_lambda_environment(ssm_env)
        # Set the variable in each lambda function
        for lambda_name in get_deployed_lambdas():
            lambda_env = get_deployed_lambda_environment(lambda_name)
            lambda_env[name] = val
            set_deployed_lambda_environment(lambda_name, lambda_env)


@params.action(
    "lambda-unset",
    arguments={
        "--name": dict(
            required=True,
            help="name of environment variable to unset (applies to all lambdas)",
        ),
        "--dry-run": dict(
            default=False,
            action="store_true",
            help="do a dry run of the actual operation",
        ),
    },
)
def lambda_unset(argv: typing.List[str], args: argparse.Namespace):
    """Unset a single environment variable into each deployed lambda"""
    if args.name:
        name = args.name

    # Unset the variable from the SSM store first
    ssm_env = get_ssm_lambda_environment()
    try:
        del ssm_env[name]
    except KeyError:
        pass
    set_ssm_lambda_environment(ssm_env)

    # Unset the variable from each lambda function
    for lambda_name in get_deployed_lambdas():
        lambda_env = get_deployed_lambda_environment(lambda_name)
        try:
            del lambda_env[name]
        except KeyError:
            pass
        set_deployed_lambda_environment(lambda_name, lambda_env)
