"""
Get and set parameters in the "environment" variable in the SSM parameter store.
These parameters are used in a way that is similar to environment variables.
"""
import os
import sys
import select
import json
import argparse

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
    if not param_name.startswith(prefix):
        param_name = f"{prefix}/{param_name}"
    return param_name


def get_ssm_environment() -> dict:
    """Get the value of the parameter named "environment" in the SSM param store"""
    prefix = get_ssm_variable_prefix()
    p = ssm_client.get_parameter(Name=f"/{prefix}/environment")
    parms = p["Parameter"]["Value"] # this is a string, so convert to dict
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


def set_ssm_parameter(environment: dict, set_env_fn, env_var: str, value) -> None:
    """
    Set a parameter in the SSM param store variable "environment".

    :param environment: a dictionary containing environment variables
    :param set_env_fn: a function handle that will set the SSM parameter variable "environment"
    :param env_var: the name of the environment variable being set
    :param value: the value of the environment variable being set
    """
    try:
        prev_value = environment[env_var]
    except:
        prev_value = None
    environment[env_var] = value
    set_env_fn(environment)
    print(f'Set variable {env_var} in SSM param store environment')
    if prev_value:
        print(f'Previous value: {prev_value}')


def unset_ssm_parameter(environment: dict, set_env_fn, env_var: str) -> None: 
    """
    Unset a parameter in the SSM param store variable "environment".

    :param environment: a dictionary containing environment variables
    :param set_env_fn: a function handle that will set the SSM parameter variable "environment"
    :param env_var: the name of the environment variable being set
    """
    try:
        prev_value = environment[env_var]
        del environment[env_var]
        env_set_fn(environment)
        print(f'Unset variable {env_var} in SSM param store environment')
        print(f'Previous value: {prev_value}')
    except KeyError:
        print(f'Nothing to unset for variable "{env_var}" in SSM param store environment')
