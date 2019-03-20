#!/usr/bin/env python
"""
Used to check missing deployed lambda env variables with local environment variables.
Does not check the values of the variables, limited to a check if the key is present.

"""
import os
import boto3
import json
import argparse


lambda_client = boto3.client("lambda")


def get_deployed_lambdas():
    root, dirs, files = next(os.walk(os.path.join(os.environ['DSS_HOME'], "daemons")))
    functions = [f"{name}-{os.environ['DSS_DEPLOYMENT_STAGE']}" for name in dirs]
    functions.append(f"dss-{os.environ['DSS_DEPLOYMENT_STAGE']}")
    for name in functions:
        try:
            resp = lambda_client.get_function(FunctionName=name)
            yield name
        except lambda_client.exceptions.ResourceNotFoundException:
           pass


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


def get_lambda_environment(aws_lamda):  # TODO change this to pass lambda names
    parms = lambda_client.get_function_configuration(
        FunctionName=aws_lamda
    )['Environment']['Variables']
    return parms


def compare_local_stage(aws_lambda: str,filter_env: list = []):
    """Compares local env values to deployed lambda end value"""
    ssm_env = [x for x in get_lambda_environment(aws_lambda) if x not in filter_env]
    local_env = [x for x in get_local_lambda_environment_keys() if x not in filter_env]
    in_ssm = [x for x in ssm_env if x not in local_env]
    in_local = [x for x in local_env if x not in ssm_env]
    return {aws_lambda: in_ssm, "local": in_local}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--special",
        default=False,
        action="store_true",
        help='checks deployment loaded env variables as well'
    )
    parser.add_argument("--aws_lambda",
        default='dss-' + os.environ['DSS_DEPLOYMENT_STAGE'],
        help='the lambda to be compared with, defaults to dss-dev'
    )
    parser.add_argument("--all",
        default=False,
        action="store_true",
        help=' checks all the aws_lambdas for the current stage'
    )
    args = parser.parse_args()
    filter_env = ['GOOGLE_APPLICATION_CREDENTIALS', 'GOOGLE_APPLICATION_SECRETS',
                  'DSS_ES_ENDPOINT', 'DSS_VERSION']
    stage = os.environ['DSS_DEPLOYMENT_STAGE']
    if args.special:
        # removes the filter to allow specials to pass
        filter_env = []
    if args.all:
        list_functions = lambda_client.list_functions()
        all_functions = list_functions['Functions']
        all_missing_keys = {}
        for aws_lambda in get_deployed_lambdas():
            print(aws_lambda)
            missing_keys = compare_local_stage(aws_lambda=aws_lambda, filter_env=filter_env)
            if any(missing_keys.values()):
                all_missing_keys[aws_lambda] = missing_keys
        if any(all_missing_keys.values()):
            print(f"Warning: Found differences in variables for : \n{json.dumps(all_missing_keys)}")
        exit()
    missing_keys = compare_local_stage(aws_lambda=args.aws_lambda, filter_env=filter_env)
    if any(missing_keys.values()):
        print(f"Warning: Found differences in variables for : \n{json.dumps(missing_keys)}")
