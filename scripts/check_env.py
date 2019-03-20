#!/usr/bin/env python
"""
Used to check missing deployed lambda env variables with local environment variables.
Does not check the values of the variables, limited to a check if the key is present.

"""
import os
import boto3
import json
import argparse


ssm_client = boto3.client("ssm")


def get_ssm_lambda_environment(stage: str):
    parms = ssm_client.get_parameter(
        Name=f"/{os.environ['DSS_PARAMETER_STORE']}/{stage}/environment"
    )['Parameter']['Value']
    return json.loads(parms)


def get_local_lambda_environment_keys():
    env = dict()
    deployment_env = ["ADMIN_USER_EMAILS"]
    deployment_env += os.environ['EXPORT_ENV_VARS_TO_LAMBDA'].split()
    deployment_env.remove('DSS_VERSION')
    for name in deployment_env:
        try:
            env[name] = os.environ[name]
        except KeyError:
            print(f"Warning: {name} not defined")
    return env.keys()


def compare_local_with_stage(stage: str, filter_env: list = []):
    """Compares local env values to deployed lambda end value"""
    ssm_env = [x for x in get_ssm_lambda_environment(stage) if x not in filter_env]
    local_env = [x for x in get_local_lambda_environment_keys() if x not in filter_env]
    in_ssm = [x for x in ssm_env if x not in local_env]
    in_local = [x for x in local_env if x not in ssm_env]
    return {stage: in_ssm, "local": in_local}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--special",
        default=False,
        action="store_true",
        help='checks deployment loaded env variables as well'
    )
    parser.add_argument("--stage",
        default=os.environ['DSS_DEPLOYMENT_STAGE'],
        help='provide stage to override'
    )
    args = parser.parse_args()
    filter_env = ['GOOGLE_APPLICATION_CREDENTIALS', 'GOOGLE_APPLICATION_SECRETS',
                  'DSS_ES_ENDPOINT', 'DSS_VERSION']
    stage = args.stage
    if args.special:
        # removes the filter to allow specials to pass
        filter_env = []
    missing_keys = compare_local_with_stage(stage=stage, filter_env=filter_env)
    if any(missing_keys.values()):
        print(f"Warning: Found differences in variables for : \n{json.dumps(missing_keys)}")
