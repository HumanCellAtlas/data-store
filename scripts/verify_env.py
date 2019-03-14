#!/usr/bin/env python
"""
Used to check missing deployed lambda env variables with local environment variables.
Does not check the values of the variables, limited to a check if the key is present.

"""
import os
import boto3
import json


lambda_client = boto3.client("lambda")


def get_local_lambda_environment_keys():
    env = dict()
    deployment_env = ["ADMIN_USER_EMAILS", "DSS_ES_ENDPOINT",
                      "GOOGLE_APPLICATION_CREDENTIALS", "GOOGLE_APPLICATION_SECRETS"]
    deployment_env += os.environ['EXPORT_ENV_VARS_TO_LAMBDA'].split()
    for name in deployment_env:
        try:
            env[name] = os.environ[name]
        except KeyError:
            print(f"Warning: {name} not defined")
    return env.keys()


def get_lambda_environment(stage):
    parms = lambda_client.get_function_configuration(
        FunctionName=f"dss-{stage}"
    )['Environment']['Variables']
    return parms


def compare_local_stage():
    """Compares local env values to deployed lambda end value"""
    stage = os.environ['DSS_DEPLOYMENT_STAGE']
    ssm_env = set(get_lambda_environment(stage))
    local_env = set(get_local_lambda_environment_keys())
    in_ssm = [ x for x in ssm_env if x not in local_env]
    in_local = [ x for x in local_env if x not in ssm_env]
    return {stage: in_ssm, "local": in_local}


if __name__ == '__main__':
    missing_keys = compare_local_stage()
    if any(missing_keys.values()):
        print(f"Warning: Found differences in variables for : \n{json.dumps(missing_keys)}")
