"""
Get and set environment variables in deployed lambda functions using the SSM param store
variable named "environment".
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
from dss.operations.ssm_params import get_ssm_variable_prefix, fix_ssm_variable_prefix
from dss.operations.ssm_params import set_ssm_parameter, unset_ssm_parameter
from dss.operations.ssm_params import set_ssm_environment

from dss.util.aws.clients import secretsmanager as sm_client  # type: ignore
from dss.util.aws.clients import es as es_client  # type: ignore
import dss.util.aws.clients
lambda_client = getattr(dss.util.aws.clients, "lambda")


logger = logging.getLogger(__name__)


def get_elasticsearch_endpoint() -> str:
    domain_name = os.environ["DSS_ES_DOMAIN"]
    domain_info = es_client.describe_elasticsearch_domain(DomainName=domain_name)
    return domain_info["DomainStatus"]["Endpoint"]


def get_admin_emails() -> str:

    def get_secret_variable_prefix() -> str:
        # TODO: this functionality should be moved to dss/operations/secrets.py
        store_name = os.environ["DSS_SECRETS_STORE"]
        stage_name = os.environ["DSS_DEPLOYMENT_STAGE"]
        store_prefix = f"{store_name}/{stage_name}"
        return store_prefix

    def fix_secret_variable_prefix(secret_name: str) -> str:
        # TODO: this functionality should be moved to dss/operations/secrets.py
        prefix = get_secret_variable_prefix()
        if not (secret_name.startswith(prefix) or secret_name.startswith("/" + prefix)):
            secret_name = f"{prefix}/{secret_name}"
        return secret_name

    def fetch_secret_safely(secret_name: str) -> dict:
        # TODO: this functionality should be moved to dss/operations/secrets.py
        try:
            response = sm_client.get_secret_value(SecretId=secret_name)
        except ClientError as e:
            if 'Error' in e.response:
                errtype = e.response['Error']['Code']
                if errtype == 'ResourceNotFoundException':
                    raise RuntimeError(f"Error: secret {secret_name} was not found!")
            raise RuntimeError(f"Error: could not fetch secret {secret_name} from secrets manager")
        else:
            return response

    gcp_var = "GOOGLE_APPLICATION_CREDENTIALS_SECRETS_NAME"
    gcp_secret_id = fix_secret_variable_prefix(gcp_var)
    response = fetch_secret_safely(gcp_secret_id)['SecretString']
    gcp_service_account_email = json.loads(response)['client_email']

    admin_var = "ADMIN_USER_EMAILS_SECRETS_NAME"
    admin_secret_id = fix_secret_variable_prefix(admin_var)
    response = fetch_secret_safely(admin_secret_id)['SecretString']
    email_list = [email.strip() for email in response.split(',') if email.strip()]

    if gcp_service_account_email not in email_list:
        email_list.append(gcp_service_account_email)
    return ",".join(email_list)


def get_deployed_lambdas(quiet: bool = True):
    """
    Generator returning names of lambda functions

    :param bool quiet: if true, don't print warnings about lambdas that can't be found
    """
    stage = os.environ["DSS_DEPLOYMENT_STAGE"]

    dirs = []
    path = os.path.join(os.environ["DSS_HOME"], "daemons")
    for item in os.scandir(path):
        if not item.name.startswith('.') and item.is_dir():
            dirs.append(item.name)

    functions = [f"{name}-{stage}" for name in dirs]
    functions.append(f"dss-{stage}")
    for name in functions:
        try:
            lambda_client.get_function(FunctionName=name)
            yield name
        except lambda_client.exceptions.ResourceNotFoundException:
            if not quiet:
                print(f"{name} not deployed, or does not deploy a lambda function")


def get_deployed_lambda_environment(lambda_name: str, quiet: bool = True) -> dict:
    """Get the environment variables in a deployed lambda function"""
    try:
        lambda_client.get_function(FunctionName=lambda_name)
        c = lambda_client.get_function_configuration(FunctionName=lambda_name)
    except lambda_client.exceptions.ResourceNotFoundException:
        if not quiet:
            print(f"{lambda_name} is not a deployed lambda function")
        return {}
    else:
        # get_function_configuration() above returns a dict, no need to convert
        return c["Environment"]["Variables"]


def set_deployed_lambda_environment(lambda_name: str, env: dict) -> None:
    """Set the environment variables in a deployed lambda function"""
    lambda_client.update_function_configuration(
        FunctionName=lambda_name, Environment={"Variables": env}
    )


def get_local_lambda_environment(quiet: bool = True) -> dict:
    """
    For each environment variable being set in deployed lambda functions, get each environment
    variable and its value from the local environment, put them in a dict, and return it.

    :param bool quiet: if true, don't print warning messages
    :returns: dict containing local environment's value of each variable exported to deployed lambda functions
    """
    env = dict()
    for name in os.environ["EXPORT_ENV_VARS_TO_LAMBDA"].split():
        try:
            env[name] = os.environ[name]
        except KeyError:
            if not quiet:
                print(
                    f"Warning: environment variable {name} is in the list of environment variables "
                    "to export to lambda functions, EXPORT_ENV_VARS_TO_LAMBDA, but variable is not "
                    "defined in the local environment, so there is no value to set."
                )
    return env


def set_lambda_var(env_var: str, value, lambda_name: str, quiet: bool = False) -> None:
    """Set a single variable in the environment of the specified lambda function"""
    environment = get_deployed_lambda_environment(lambda_name, quiet=False)
    environment[env_var] = value
    set_deployed_lambda_environment(lambda_name, environment)
    if not quiet:
        print(f"Success! Set variable {env_var} in deployed lambda function {lambda_name}")


def unset_lambda_var(env_var: str, lambda_name: str, quiet: bool = False) -> None:
    """Unset a single variable in the environment of the specified lambda function"""
    environment = get_deployed_lambda_environment(lambda_name, quiet=False)
    try:
        prev_value = environment[env_var]
        del environment[env_var]
        set_deployed_lambda_environment(lambda_name, environment)
        if not quiet:
            print(f"Success! Unset variable {env_var} in deployed lambda function {lambda_name}")
    except KeyError:
        if not quiet:
            print(f"Nothing to unset for variable {env_var} in deployed lambda function {lambda_name}")


def print_lambda_env(lambda_name, lambda_env):
    """Print the environment variables set in a specified lambda function"""
    print(f"\n{lambda_name}:")
    for name, val in lambda_env.items():
        print(f"{name}={val}")


lambda_params = dispatch.target("lambda", arguments={}, help=__doc__)


json_flag_options = dict(
    default = False, action="store_true", help="format the output as JSON if this flag is present"
)
dryrun_flag_options = dict(
    default = False, action="store_true", help="do a dry run of the actual operation"
)
quiet_flag_options = dict(
    default = False, action="store_true", help="suppress output"
)


@lambda_params.action(
    "list",
    arguments={
        "--json": json_flag_options,
    }
)
def lambda_list(argv: typing.List[str], args: argparse.Namespace):
    """Print a list of names of each deployed lambda function"""
    lambda_names = list(get_deployed_lambdas(quiet=args.json))
    if args.json:
        print(json.dumps(lambda_names, indent=4, default=str))
    else:
        for lambda_name in lambda_names:
            print(lambda_name)


@lambda_params.action(
    "environment",
    arguments={
        "--json": json_flag_options,
        "--lambda-name": dict(
            required=False,
            help="specify the name of a lambda function whose environment will be listed"
        )
    }
)
def lambda_environment(argv: typing.List[str], args: argparse.Namespace):
    """Print out the current environment of deployed lambda functions"""
    if args.lambda_name:
        lambda_names = [args.lambda_name]  # single lambda function
    else:
        lambda_names = list(get_deployed_lambdas())  # all lambda functions

    # Iterate over lambda functions and get their environments
    d = {}
    for lambda_name in lambda_names:
        lambda_env = get_deployed_lambda_environment(lambda_name, quiet=args.json)
        d[lambda_name] = lambda_env

    # Print environments
    if args.json:
        print(json.dumps(d, indent=4, default=str))
    else:
        for lambda_name, lambda_env in d.items():
            print_lambda_env(lambda_name, lambda_env)


@lambda_params.action(
    "set",
    arguments={
        "name": dict(help="name of variable to set in environment of deployed lambdas"),
        "--dry-run": dryrun_flag_options,
        "--quiet": quiet_flag_options
    }
)
def lambda_set(argv: typing.List[str], args: argparse.Namespace):
    """Set an environment variable in each deployed lambda"""
    name = args.name

    # Use stdin for value
    if not select.select([sys.stdin], [], [], 0.0)[0]:
        raise RuntimeError("Error: stdin was empty! A variable value must be provided via stdin")
    val = sys.stdin.read()

    if args.dry_run:
        if not args.quiet:
            print(f"Dry-run setting variable {name} in lambda environment in SSM store under "
                   "$DSS_DEPLOYMENT_STAGE/environment")
            print(f"    Name: {name}")
            print(f"    Value: {val}")
            for lambda_name in get_deployed_lambdas():
                print(f"Dry-run setting variable {name} in lambda {lambda_name}")

    else:
        # Set the variable in the SSM store first, then in each deployed lambda
        set_ssm_parameter(name, val, quiet=args.quiet)
        for lambda_name in get_deployed_lambdas():
            set_lambda_var(name, val, lambda_name, quiet=args.quiet)


@lambda_params.action(
    "unset",
    arguments={
        "name": dict(help="name of variable to unset in environment of deployed lambdas"),
        "--dry-run": dryrun_flag_options,
        "--quiet": quiet_flag_options
    }
)
def lambda_unset(argv: typing.List[str], args: argparse.Namespace):
    """Unset an environment variable in each deployed lambda"""
    name = args.name

    # Unset the variable from the SSM store first, then from each deployed lambda
    if args.dry_run:
        if not args.quiet:
            print(f'Dry-run deleting variable {name} from SSM store')
            for lambda_name in get_deployed_lambdas():
                print(f'Dry-run deleting variable {name} from lambda function "{lambda_name}"')

    else:
        unset_ssm_parameter(name, quiet=args.quiet)
        for lambda_name in get_deployed_lambdas():
            unset_lambda_var(name, lambda_name, quiet=args.quiet)


@lambda_params.action(
    "update",
    arguments={
        "--update-deployed": dict(
            default=False,
            action="store_true",
            help="update the environment variables of all deployed lambdas, in addition to "
            "updating the lambda environment stored in SSM store under $DSS_DEPLOYMENT_STAGE/environment",
        ),
        "--force": dict(
            default=False,
            action="store_true",
            help="force the action to happen (no interactive prompt)",
        ),
        "--dry-run": dryrun_flag_options,
        "--quiet": quiet_flag_options
    }
)
def lambda_update(argv: typing.List[str], args: argparse.Namespace):
    """
    Update the stored (and optionally, deployed) lambda environment
    using variable values from the current environment.
    """
    if args.force is False:
        # Make sure the user really wants to do this
        comfirm  = f"""
        *** WARNING!!! ***

        Calling the lambda update function will overwrite the current
        values of the lambda function environment stored in the
        SSM store at $DSS_DEPLOY_STAGE/environment with local
        values from environment variables on your machine.

        Note:
        - To do a dry run of this operation first, use the --dry-run flag.
        - To ignore this warning, use the --force flag.
        - To see the current environment stored in the SSM store, run:
            ./dss-ops.py lambda environment

        Are you really sure you want to update the SSM store environment?
        (Type 'y' or 'yes' to confirm):
        """
        response = input(confirm)
        if response.lower() not in ["y", "yes"]:
            raise RuntimeError("You safely aborted the lambda update operation!")

    # Only elasticsearch endpoint and admin emails are updated dynamically,
    # everything else comes from the local environment.
    local_env = get_local_lambda_environment()
    local_env["DSS_ES_ENDPOINT"] = get_elasticsearch_endpoint()
    local_env["ADMIN_USER_EMAILS"] = get_admin_emails()

    if args.dry_run:
        if not args.quiet:
            print(f"Dry-run redeploying local environment to lambda function environment "
                   "stored in SSM store under $DSS_DEPLOYMENT_STAGE/environment")
    else:
        set_ssm_environment(local_env)
        if not args.quiet:
            print(f"Finished redeploying local environment to lambda function environment "
                    "stored in SSM store under $DSS_DEPLOY_STAGE/environment")

    # Optionally, update environment of each deployed lambda
    if args.update_deployed:
        for lambda_name in get_deployed_lambdas():
            # Add the new variable to each lambda's environment
            lambda_env = get_deployed_lambda_environment(lambda_name)
            lambda_env.update(local_env)
            if args.dry_run:
                if not args.quiet:
                    print(f"Dry-run redeploying lambda function environment from SSM store for {lambda_name}")
            else:
                set_deployed_lambda_environment(lambda_name, lambda_env)
                if not args.quiet:
                    print(f"Finished redeploying lambda function environment from SSM store for {lambda_name}")
