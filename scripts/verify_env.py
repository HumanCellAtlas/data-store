#!/usr/bin/env python
"""
Used to check missing deployed lambda env variables with local environment variables.
Does not check the values of the variables, limited to a check if they key is present.

"""
import os
import boto3
import json


ssm_client = boto3.client("ssm")


def get_local_lambda_environment_keys():
    env = ["ADMIN_USER_EMAILS", "DSS_ES_ENDPOINT", "GOOGLE_APPLICATION_CREDENTIALS", "GOOGLE_APPLICATION_SECRETS"]
    for name in os.environ['EXPORT_ENV_VARS_TO_LAMBDA'].split():
            env.append(name)
    return env


def get_ssm_lambda_environment(stage):
    parms = ssm_client.get_parameter(
        Name=f"/{os.environ['DSS_PARAMETER_STORE']}/{stage}/environment"
    )['Parameter']['Value']
    return json.loads(parms)


def compare_local_stage(stage=None):
    """Compares local env values to deployed values"""
    if stage is None:
        stage = os.environ['DSS_DEPLOYMENT_STAGE']
    ssm_env = set(get_ssm_lambda_environment(stage))
    local_env = set(get_local_lambda_environment_keys())
    in_ssm = list(ssm_env - local_env)
    in_local = list(local_env - ssm_env)
    return {stage: in_ssm, "local": in_local}


def compare_env_lists(l1: list, l2: list):
    return list(set.symmetric_difference(set(l1), set(l2)))


if __name__ == '__main__':
    missing_keys = json.dumps(compare_local_stage())
    if len(missing_keys) > 0:
        print(f"Warning: Missing Environment Variables Found: \n{missing_keys}")
