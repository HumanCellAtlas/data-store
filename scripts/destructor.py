#!/usr/bin/env python

"""
This script provides tools useful for removing some AWS and GCP services unmanaged by Terraform:
    AWS: Lamdas, stepfunctions, api gateways, iam roles 
    GCP: service accounts, cloud functions

The components of a DSS deployment listed above may be removed with the `destruct` command,
provided the components are suffixed with the stage name, e.g. `{component-name}-{stage}`
"""

import json
import boto3
import click
import subprocess


IAM = boto3.client('iam')
LAMBDA = boto3.client('lambda')
SFN = boto3.client('stepfunctions')
APIGATEWAY = boto3.client('apigateway')


def cache_filter(func):
    cache = {}
    def wrapped(name=None, prefix='', suffix=''):
        if not cache.get(func, None):
            cache[func] = func()
        results = cache[func]

        if name is not None:
            if name in results:
                return {name: results[name]}
            else:
                return {} 

        return {name: results[name]
                for name in results
                if name.startswith(prefix) and name.endswith(suffix)}

    return wrapped 


def run(command, quiet=False):
    if not quiet:
        print(command)

    out = subprocess.run(command,
                         shell=True,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE,
                         encoding='utf-8')

    try:
        out.check_returncode()
    except subprocess.CalledProcessError:
        if not quiet:
            print(f'\t{out.stderr}')
            print(f'Exit status {out.returncode} while running "{command}"')
    return out.stdout.strip()


def confirm(kind, name):
    if not click.confirm(f'Delete {kind} {name}?'):
        return False

#    if name != click.prompt(f'Enter {kind} name to confirm', default='', show_default=False):
#        return False

    return True


@cache_filter
def deployed_roles():
    paginator = IAM.get_paginator('list_roles')
    return {r['RoleName'] : r['Arn']
            for page in paginator.paginate(MaxItems=100)
            for r in page['Roles']}


@cache_filter
def deployed_lambdas():
    paginator = LAMBDA.get_paginator('list_functions')
    return {l['FunctionName'] : l['FunctionArn']
            for page in paginator.paginate(MaxItems=100)
            for l in page['Functions']}


@cache_filter
def deployed_stepfunctions():
    paginator = SFN.get_paginator('list_state_machines')
    return {sfn['name'] : sfn['stateMachineArn']
            for page in paginator.paginate(maxResults=100)
            for sfn in page['stateMachines']}


@cache_filter
def deployed_gcp_service_accounts():
    service_accounts = json.loads(run('gcloud iam service-accounts list --format json', quiet=True))
    return {sa['email'].split('@')[0] : sa
            for sa in service_accounts}


@cache_filter
def deployed_api_gateways():
    gateways = dict()

    for api in APIGATEWAY.get_rest_apis()['items']:
        api_id = api['id']

        for resource in APIGATEWAY.get_resources(restApiId=api_id)['items']:
            try:
                info = APIGATEWAY.get_integration(restApiId=api_id, resourceId=resource['id'], httpMethod='GET')
            except APIGATEWAY.exceptions.ClientError as e:
                if e.response['Error']['Code'] == 'NotFoundException':
                    continue
                else:
                    raise

            kind, name = info['uri'].split(':')[-2:]
            name = name.split('/')[0]
            gateways[name] = api_id
            break

    return gateways


def raw_api_host(stage, prefix='dss'):
    try:
        resp = LAMBDA.get_policy(FunctionName=f'{prefix}-{stage}') 
    except LAMBDA.exceptions.ClientError:
        raise Exception(f'{prefix}-{stage} not found among api gateway deployments')

    region = boto3.session.Session().region_name
    policy = json.loads(resp['Policy'])
    arn = policy['Statement'][0]['Condition']['ArnLike']['AWS:SourceArn']
    api_id = arn.split(':')[-1].split('/')[0]
    
    return f'{api_id}.execute-api.{region}.amazonaws.com/{stage}/'


def delete_api_gateway(name):
    print()
    if confirm('api gateway', name):
        api_id = deployed_api_gateways().get(name, None)
        if api_id is not None:
            APIGATEWAY.delete_rest_api(restApiId=api_id)
            print(f'deleted {name}')


def delete_api_gateways(name, prefix, suffix):
    for name, api_id in deployed_api_gateways(name, prefix, suffix).items():
        delete_api_gateway(name)


def delete_role(name):
    print()
    if confirm('iam role', name):
        IAM.delete_role_policy(RoleName=name, PolicyName=name)
        IAM.delete_role(RoleName=name)
        print(f'deleted {name}')


def delete_roles(name, prefix, suffix):
    for name in deployed_roles(name, prefix, suffix):
        delete_role(name)


def delete_lambda(name):
    print()
    if confirm('lambda', name):
        LAMBDA.delete_function(FunctionName=name)
        print(f'deleted {name}')


def delete_lambdas(name, prefix, suffix):
    for name in deployed_lambdas(name, prefix, suffix):
        delete_lambda(name)


def delete_stepfunction(name):
    print()
    if confirm('stepfunction', name):
        arn = deployed_stepfunctions().get(name, None)
        if arn is not None:
            SFN.delete_state_machine(stateMachineArn=arn)
            print(f'deleted {name}')


def delete_stepfunctions(name, prefix, suffix):
    for name in deployed_stepfunctions(name, prefix, suffix):
        delete_stepfunction(name)


def delete_google_cloud_function(name):
    print()
    functions = json.loads(run('gcloud beta functions list --format json', quiet=True))
    functions = [f['name'].split('/')[-1] for f in functions]
    if name not in functions:
        print(f'google cloud function {name} not deployed')
    else:
        run(f'gcloud beta functions delete {name} --quiet')


def delete_gcp_service_account(name):
    print()
    if not confirm('gcp service account', name):
        return

    project_id = run('gcloud config get-value project', quiet=True)
    email = f'{name}@{project_id}.iam.gserviceaccount.com'
    member = f'serviceAccount:{email}'
    policy = json.loads(run(f'gcloud projects get-iam-policy {project_id} --format json', quiet=True))
    bindings = policy['bindings']
    for b in bindings:
        if member in b['members']:
            role = b['role']
            run(f'gcloud projects remove-iam-policy-binding {project_id} --member {member} --role {role}')
    run(f'gcloud iam service-accounts delete {email} --quiet')
        

def delete_gcp_service_accounts(name, prefix, suffix):
    for name in deployed_gcp_service_accounts(name, prefix, suffix):
        delete_gcp_service_account(name)


@click.command('delete_api_gateways')
@click.option('--name', default=None)
@click.option('--prefix', default='')
@click.option('--suffix', default='')
def delete_api_gateways_command(name, prefix, suffix):
    delete_api_gateways(name, prefix, suffix)


@click.command('delete_lambdas')
@click.option('--name', default=None)
@click.option('--prefix', default='')
@click.option('--suffix', default='')
def delete_lambdas_command(name, prefix, suffix):
    delete_lambdas(name, prefix, suffix)


@click.command('delete_stepfunctions')
@click.option('--name', default=None)
@click.option('--prefix', default='')
@click.option('--suffix', default='')
def delete_stepfunctions_command(name, prefix, suffix):
    delete_stepfunctions(name, prefix, suffix)


@click.command('delete_roles')
@click.option('--name', default=None)
@click.option('--prefix', default='')
@click.option('--suffix', default='')
def delete_roles_command(name, prefix, suffix):
    delete_roles(name, prefix, suffix)


@click.command('delete_gcp_service_accounts')
@click.option('--name', default=None)
@click.option('--prefix', default='')
@click.option('--suffix', default='')
def delete_gcp_service_accounts_command(name, prefix, suffix):
    delete_gcp_service_accounts(name, prefix, suffix)


@click.command('destruct')
@click.argument('stage')
def destruct_command(stage):
    delete_api_gateways(name=None, prefix='', suffix=stage)
    delete_lambdas(name=None, prefix='', suffix=stage)
    delete_stepfunctions(name=None, prefix='', suffix=stage)
    delete_roles(name=None, prefix='', suffix=stage)
    delete_gcp_service_accounts(name=None, prefix='', suffix=stage)
    delete_google_cloud_function(f'dss-gs-event-relay-{stage}')


@click.command('raw_api_host')
@click.argument('stage')
@click.option('--prefix', default='dss')
def raw_api_host_command(stage, prefix):
    print(raw_api_host(stage, prefix))


@click.group()
def cli():
    pass


if __name__ == "__main__":
    cli.add_command(destruct_command)
    cli.add_command(delete_api_gateways_command)
    cli.add_command(delete_lambdas_command)
    cli.add_command(delete_stepfunctions_command)
    cli.add_command(delete_roles_command)
    cli.add_command(delete_gcp_service_accounts_command)
    cli.add_command(raw_api_host_command)
    cli()
