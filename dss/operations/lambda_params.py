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
from dss.operations.secrets import fix_secret_variable_prefix, fetch_secret_safely

from dss.util.aws.clients import secretsmanager as sm_client  # type: ignore
from dss.util.aws.clients import es as es_client  # type: ignore
from dss.util.aws.clients import ssm as ssm_client  # type: ignore
import dss.util.aws.clients
lambda_client = getattr(dss.util.aws.clients, "lambda")


logger = logging.getLogger(__name__)


# ---
# Utility functions for SSM parameter store:
# ---
def get_ssm_variable_prefix() -> str:
    """
    Use info from local environment to assemble necessary prefix for environment variables stored
    in the SSM param store under $DSS_DEPLOYMENT_STAGE/environment
    """
    store_name = os.environ["DSS_PARAMETER_STORE"]
    stage_name = os.environ["DSS_DEPLOYMENT_STAGE"]
    store_prefix = f"{store_name}/{stage_name}"
    return store_prefix


def fix_ssm_variable_prefix(param_name: str) -> str:
    """Add (if necessary) the variable store and stage prefix to the front of the name of an SSM store parameter"""
    prefix = get_ssm_variable_prefix()
    if not (param_name.startswith(prefix) or param_name.startswith("/" + prefix)):
        param_name = f"{prefix}/{param_name}"
    return param_name


def get_ssm_environment() -> dict:
    """Get the value of the environment variables stored in the SSM param store under $DSS_DEPLOYMENT_STAGE/environment"""
    p = ssm_client.get_parameter(Name=fix_ssm_variable_prefix("environment"))
    parms = p["Parameter"]["Value"]  # this is a string, so convert to dict
    return json.loads(parms)


def set_ssm_environment(env: dict) -> None:
    """
    Set the value of environment variables stored in the SSM param store under $DSS_DEPLOYMENT_STAGE/environment

    :param env: dict containing environment variables to set in SSM param store
    :returns: nothing
    """
    prefix = get_ssm_variable_prefix()
    ssm_client.put_parameter(
        Name=f"/{prefix}/environment", Value=json.dumps(env), Type="String", Overwrite=True
    )


def set_ssm_parameter(env_var: str, value, quiet: bool = False) -> None:
    """
    Set a variable in the lambda environment stored in the SSM store under $DSS_DEPLOYMENT_STAGE/environment

    :param env_var: the name of the environment variable being set
    :param value: the value of the environment variable being set
    :param bool quiet: suppress all output if true
    """
    environment = get_ssm_environment()
    prev_value = environment.get(env_var)
    environment[env_var] = value
    set_ssm_environment(environment)
    if not quiet:
        print("Success! Set variable in SSM parameter store environment:")
        print(f"    Name: {env_var}")
        print(f"    Value: {value}")
        if prev_value:
            print(f"Previous value: {prev_value}")


def unset_ssm_parameter(env_var: str, quiet: bool = False) -> None:
    """
    Unset a variable in the lambda environment stored in the SSM store undre $DSS_DEPLOYMENT_STAGE/environment

    :param env_var: the name of the environment variable being set
    :param value: the value of the environment variable being set
    :param bool quiet: suppress all output if true
    """
    environment = get_ssm_environment()
    try:
        prev_value = environment[env_var]
        del environment[env_var]
        set_ssm_environment(environment)
        if not quiet:
            print("Success! Unset variable in SSM store under $DSS_DEPLOYMENT_STAGE/environment:")
            print(f"    Name: {env_var} ")
            print(f"    Previous value: {prev_value}")
    except KeyError:
        if not quiet:
            print(f"Nothing to unset for variable {env_var} in SSM store under $DSS_DEPLOYMENT_STAGE/environment")


# ---
# Utility functions for lambda functions:
# ---
def get_elasticsearch_endpoint() -> str:
    domain_name = os.environ["DSS_ES_DOMAIN"]
    domain_info = es_client.describe_elasticsearch_domain(DomainName=domain_name)
    return domain_info["DomainStatus"]["Endpoint"]


def get_admin_emails() -> str:
    gcp_var = os.environ["GOOGLE_APPLICATION_CREDENTIALS_SECRETS_NAME"]
    gcp_secret_id = fix_secret_variable_prefix(gcp_var)
    response = fetch_secret_safely(gcp_secret_id)['SecretString']
    gcp_service_account_email = json.loads(response)['client_email']

    admin_var = os.environ["ADMIN_USER_EMAILS_SECRETS_NAME"]
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


# ---
# Command line utility functions
# ---
lambda_params = dispatch.target("lambda", arguments={}, help=__doc__)
ssm_params = dispatch.target("params", arguments={}, help=__doc__)


json_flag_options = dict(
    default=False, action="store_true", help="format the output as JSON if this flag is present"
)
dryrun_flag_options = dict(
    default=False, action="store_true", help="do a dry run of the actual operation"
)
quiet_flag_options = dict(
    default=False, action="store_true", help="suppress output"
)


@ssm_params.action(
    "environment",
    arguments={
        "--json": dict(
            default=False, action="store_true", help="format the output as JSON if this flag is present"
        )
    },
)
def ssm_environment(argv: typing.List[str], args: argparse.Namespace):
    """Print out all variables stored in the SSM store under $DSS_DEPLOYMENT_STAGE/environment"""
    ssm_env = get_ssm_environment()
    if args.json:
        print(json.dumps(ssm_env, indent=4))
    else:
        for name, val in ssm_env.items():
            print(f"{name}={val}")
        print("\n")


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
        if lambda_env != {}:
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
        "--quiet": quiet_flag_options,
    }
)
def lambda_set(argv: typing.List[str], args: argparse.Namespace):
    """
    Set a variable in the SSM store under $DSS_DEPLOYMENT_STAGE/environment,
    then set the variable in each deployed lambda.
    """
    name = args.name

    # Use stdin for value
    if not select.select([sys.stdin], [], [], 0.0)[0]:
        raise RuntimeError("Error: stdin was empty! A variable value must be provided via stdin")
    val = sys.stdin.read()

    if args.dry_run:
        if not args.quiet:
            print(
                f"Dry-run setting variable {name} in lambda environment in SSM store under "
                "$DSS_DEPLOYMENT_STAGE/environment"
            )
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
    """
    Unset a variable in the SSM store under $DSS_DEPLOYMENT_STAGE/environment,
    then unset the variable in each deployed lambda.
    """
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
    Update the lambda environment stored in the SSM store under $DSS_DEPLOYMENT_STAGE/environment
    by taking values from the current (local) environment. If --update-deployed flag is provided,
    also update the environment of deployed lambda functions.
    """
    if not args.force and not args.dry_run:
        # Prompt the user to make sure they really want to do this
        confirm = f"""
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
            print(
                f"Dry-run redeploying local environment to lambda function environment "
                "stored in SSM store under $DSS_DEPLOYMENT_STAGE/environment"
            )
    else:
        set_ssm_environment(local_env)
        if not args.quiet:
            print(
                f"Finished redeploying local environment to lambda function environment "
                "stored in SSM store under $DSS_DEPLOY_STAGE/environment"
            )

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
