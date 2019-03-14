#!/usr/bin/env python
"""
Used to check missing deployed lambda env variables with local environment variables.
Does not check

"""
import os
import boto3
import json


ssm_client = boto3.client("ssm")
stages = ['dev', 'integration', 'staging', 'prod']


def get_local_lambda_environment_keys():
    env = ["ADMIN_USER_EMAILS", "DSS_ES_ENDPOINT", "GOOGLE_APPLICATION_CREDENTIALS", "GOOGLE_APPLICATION_SECRETS"]
    for name in os.environ['EXPORT_ENV_VARS_TO_LAMBDA'].split():
            env.append(name)
    return env


def get_ssm_lambda_environment(stage=None):
    if stage is None:
        stage = os.environ['DSS_DEPLOYMENT_STAGE']
    parms = ssm_client.get_parameter(
        Name=f"/{os.environ['DSS_PARAMETER_STORE']}/{stage}/environment"
    )['Parameter']['Value']
    return json.loads(parms)


def compare_local_stages(stage=None):
    """Compares local env values to deployed values"""
    ssm_env = get_ssm_lambda_environment(stage)
    local_env = get_local_lambda_environment_keys()
    return compare_env_lists(local_env, ssm_env.keys())


def compare_env_lists(l1: list, l2: list):
    return list(set.symmetric_difference(set(l1), set(l2)))


if __name__ == '__main__':
    print(compare_local_stages())
