#!/usr/bin/env python
"""
This script compiles $EXPORT_ENV_VARS_TO_LAMBDA into a json document and
uploads it into AWS Systems Manager Parameter Store under the key
`dcp/dss/{DSS_DEPLOYMENT_STAGE}/environment`, optionally updating
the environment of every deployed lambda.

Individual environment variables may also be set and unset acrross both SSM and
deployed lambdas.
"""
import os
import sys
import json
import boto3
import argparse

ssm_client = boto3.client("ssm")
es_client = boto3.client("es")
lambda_client = boto3.client("lambda")

def get_local_lambda_environment():
    env = dict()
    for name in os.environ['EXPORT_ENV_VARS_TO_LAMBDA'].split():
        try:
            env[name] = os.environ[name]
        except KeyError:
            print(f"Warning: {name} not defined")
    return env

def get_ssm_lambda_environment():
    parms = ssm_client.get_parameter(
        Name=f"/{os.environ['DSS_PARAMETER_STORE']}/{os.environ['DSS_DEPLOYMENT_STAGE']}/environment"
    )['Parameter']['Value']
    return json.loads(parms)

def set_ssm_lambda_environment(parms: dict):
    ssm_client.put_parameter(
        Name=f"/dcp/dss/{os.environ['DSS_DEPLOYMENT_STAGE']}/environment",
        Value=json.dumps(parms),
        Type="String",
        Overwrite=True,
    )

def get_deployed_lambda_environment(name):
    return lambda_client.get_function_configuration(FunctionName=name)['Environment']['Variables']

def set_deployed_lambda_environment(name, env: dict):
    lambda_client.update_function_configuration(
        FunctionName=name,
        Environment={
            'Variables': env
        }
    )

def get_deployed_lambdas():
    root, dirs, files = next(os.walk(os.path.join(os.environ['DSS_HOME'], "daemons")))
    functions = [f"{name}-{os.environ['DSS_DEPLOYMENT_STAGE']}" for name in dirs]
    functions.append(f"dss-{os.environ['DSS_DEPLOYMENT_STAGE']}")
    for name in functions:
        try:
            resp = lambda_client.get_function(FunctionName=name)
            yield name
        except lambda_client.exceptions.ResourceNotFoundException:
            print(f"{name} not deployed, or does not deploy a Lambda function")

def get_elasticsearch_endpoint():
    domain_name = os.environ['DSS_ES_DOMAIN']
    domain_info = es_client.describe_elasticsearch_domain(DomainName=domain_name)
    return domain_info['DomainStatus']['Endpoint']

def get_admin_user_emails():
    secret_base = "{}/{}/".format(
        os.environ['DSS_SECRETS_STORE'],
        os.environ['DSS_DEPLOYMENT_STAGE'])

    gcp_secret_id = secret_base + os.environ['GOOGLE_APPLICATION_CREDENTIALS_SECRETS_NAME']
    admin_secret_id = secret_base + os.environ['ADMIN_USER_EMAILS_SECRETS_NAME']
    resp = boto3.client("secretsmanager").get_secret_value(SecretId=gcp_secret_id)
    gcp_service_account_email = json.loads(resp['SecretString'])['client_email']
    resp = boto3.client("secretsmanager").get_secret_value(SecretId=admin_secret_id)
    admin_user_emails = [email for email in resp['SecretString'].split(',') if email.strip()]
    admin_user_emails.append(gcp_service_account_email)
    return ",".join(admin_user_emails)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--update-deployed-lambdas",
        default=False,
        action="store_true",
        help="update the environment of all deployed lambdas"
    )
    parser.add_argument("-p", "--print",
        default=False,
        action="store_true",
        help="Display the current environment stored in SSM"
    )
    parser.add_argument("--set",
        default=None,
        help="Set a single environment variable in SSM parameters and all deployed lambdas"
    )
    parser.add_argument("--unset",
        default=None,
        help="Remove a single environment variable in SSM parameters and all deployed lambdas"
    )
    args = parser.parse_args()

    if args.print:
        ssm_env = get_ssm_lambda_environment()
        for name, val in ssm_env.items():
            print(f"{name}={val}")
    elif args.set is not None:
        name, val = args.set.split("=")
        ssm_env = get_ssm_lambda_environment()
        ssm_env[name] = val
        set_ssm_lambda_environment(ssm_env)
        for lambda_name in get_deployed_lambdas():
            lambda_env = get_deployed_lambda_environment(lambda_name)
            lambda_env[name] = val
            set_deployed_lambda_environment(lambda_name, lambda_env)
    elif args.unset is not None:
        name = args.unset
        ssm_env = get_ssm_lambda_environment()
        try:
            del ssm_env[name]
        except KeyError:
            pass
        set_ssm_lambda_environment(ssm_env)
        for lambda_name in get_deployed_lambdas():
            lambda_env = get_deployed_lambda_environment(lambda_name)
            try:
                del lambda_env[name]
            except KeyError:
                pass
            set_deployed_lambda_environment(lambda_name, lambda_env)
    else:
        lambda_env = get_local_lambda_environment()
        lambda_env['DSS_ES_ENDPOINT'] = get_elasticsearch_endpoint()
        lambda_env['ADMIN_USER_EMAILS'] = get_admin_user_emails()
        set_ssm_lambda_environment(lambda_env)
        if args.update_deployed_lambdas:
            for lambda_name in get_deployed_lambdas():
                current_lambda_env = get_deployed_lambda_environment(lambda_name)
                current_lambda_env.update(lambda_env)
                set_deployed_lambda_environment(lambda_name, current_lambda_env)
