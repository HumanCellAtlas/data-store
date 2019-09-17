"""
Get and set parameters in the SSM parameter store
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
    """Use info from environment to assemble necessary prefix for SSM parameter store variables"""
    store_name = os.environ["DSS_PARAMETER_STORE"]
    stage_name = os.environ["DSS_DEPLOYMENT_STAGE"]
    store_prefix = f"{store_name}/{stage_name}"
    return store_prefix


def fix_ssm_variable_prefix(param_name: str) -> str:
    """Adds the variable store and stage prefix to the front of an ssm parameter name"""
    prefix = get_ssm_variable_prefix()
    if not param_name.startswith(prefix):
        param_name = f"{prefix}/{param_name}"
    return param_name


