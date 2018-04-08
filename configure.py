#!/usr/bin/env python

import os
import sys
import copy
import enum
import click 
import subprocess
import dss_deployment


pkg_root = os.path.abspath(os.path.dirname(__file__))  # noqa


class Accept(enum.Enum):
    all = enum.auto()
    all_but_none = enum.auto()
    nothing = enum.auto()


def run(command):
    out = subprocess.run(command,
                         shell=True,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE,
                         encoding='utf-8')
    try:
        out.check_returncode()
    except subprocess.CalledProcessError:
        raise Exception(f'\t{out.stderr}')
    return out.stdout.strip()


def request_input(info, key, stage, accept):
    if info[key]['default'] is not None:
        default = info[key]['default'].format(stage=stage)
    else:
        default = None

    if Accept.all == accept:
        print(f'setting {key}={default}')
        info[key]['default'] = default
    elif Accept.all_but_none == accept and default is not None:
        print(f'setting {key}={default}')
        info[key]['default'] = default
    else:
        print()
        if info[key]['description']:
            print(info[key]['description'])
        val = click.prompt(f'{key}=', default)
        if 'none' == val.lower():
            val = None
        info[key]['default'] = val


def get_user_input(deployment, accept):
    if not deployment.variables['gcp_project']['default']:
        deployment.variables['gcp_project']['default'] = run("gcloud config get-value project")

    if not deployment.variables['gcp_service_account_id']['default']:
        deployment.variables['gcp_service_account_id']['default'] = f'service-account-{deployment.stage}'

    print(deployment.variables['API_DOMAIN_NAME'])

    skip = ['DSS_DEPLOYMENT_STAGE']
    for key in deployment.variables:
        if key in skip:
            continue
        request_input(deployment.variables, key, deployment.stage, accept)


@click.command()
@click.option('--stage', prompt="Deployment stage name")
@click.option('--accept-defaults', is_flag=True, default=False)
def main(stage, accept_defaults):
    deployment = dss_deployment.DSSDeployment(stage)
    exists = os.path.exists(deployment.root)

    if exists and accept_defaults:
        accept = Accept.all
    elif accept_defaults:
        accept = Accept.all_but_none
    else:
        accept = Accept.nothing

    get_user_input(deployment, accept)

    deployment.write()
    dss_deployment.set_active_stage(stage)

    print()
    print('Deployment Steps')
    print('\t1. Customize Terraform scripting as needed:')
    for comp in os.listdir(deployment.root):
        path = os.path.join(deployment.root, comp)
        if not os.path.isdir(path):
            continue
        print(f'\t\t{path}')
    print('\t2. run `scripts/create_config_gs_service_account.sh`')
    print('\t3. Visit the google console to aquire `application_secrets.json`')
    print('\t4. run `source environment`')
    print('\t5. run `make deploy-infra`')
    print('\t6. run `make deploy`')


if __name__ == "__main__":
    main()
