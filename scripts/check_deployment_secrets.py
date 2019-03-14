#!/usr/bin/env python
"""
Script to ensure that the secrets in an HCA stage deployment are not accidentally
changed to personal user credentials (or otherwise).  Requires Terraform.

Will only check the canoncial HCA stages ('dev', 'integration', 'staging', 'prod').

Run to check current deployment:
    `scripts/check_deployment_secrets.py`

Run to check dev:
    `scripts/check_deployment_secrets.py dev`

Checking occurs as follows:

#1
For the json returned from the secret in GOOGLE_APPLICATION_SECRETS_SECRETS_NAME:
    `auth_uri` should be in ['https://auth.data.humancellatlas.org/authorize',
                             'https://auth.dev.data.humancellatlas.org/authorize']
    `token_uri` should be in ['https://auth.data.humancellatlas.org/oauth/token',
                              'https://auth.dev.data.humancellatlas.org/oauth/token']

#2
For the json returned from the secret in GOOGLE_APPLICATION_CREDENTIALS_SECRETS_NAME:
    `project_id` should be `human-cell-atlas-travis-test`
    `type` should be `service_account`
    `client_email` should be the user account returned from the terraform output "service_account".
                   For example: dev should be `travis-test@human-cell-atlas-travis-test.iam.gserviceaccount.com`.
"""
import subprocess
import os
import sys
import json


class SecretsChecker(object):
    def __init__(self, stage):
        self.stage = stage
        self.stages = ('dev', 'integration', 'staging', 'prod')
        if self.stage not in self.stages:
            print('Custom stage provided.  Secret checking will be skipped.')
        self.service_account = self.fetch_terraform_output("service_account", "gcp_service_account").strip()

        self.email = [f'{self.service_account}@human-cell-atlas-travis-test.iam.gserviceaccount.com']
        self.project = ['human-cell-atlas-travis-test']
        self.type = ['service_account']
        self.auth_uri = ['https://auth.data.humancellatlas.org/authorize',
                         'https://auth.dev.data.humancellatlas.org/authorize']
        self.token_uri = ['https://auth.data.humancellatlas.org/oauth/token',
                          'https://auth.dev.data.humancellatlas.org/oauth/token']

        self.app_secret_name = os.environ['GOOGLE_APPLICATION_SECRETS_SECRETS_NAME']
        self.gcp_cred_secret_name = os.environ['GOOGLE_APPLICATION_CREDENTIALS_SECRETS_NAME']
        self.app_secret = self.fetch_secret(self.app_secret_name)
        self.gcp_cred_secret = self.fetch_secret(self.gcp_cred_secret_name)

        self.missing_secrets = []
        self.incomplete_secrets = []
        self.error_message = f'\n\n' \
                             f'Deploying to {self.stage.upper()} could not be completed.\n' \
                             f'It looks like an AWS secret has an unexpected value.\n' \
                             f'Please do not change AWS secrets for releases.\n'

    @staticmethod
    def run_cmd(cmd, cwd=os.getcwd()):
        p = subprocess.Popen(cmd,
                             shell=True,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             cwd=cwd)
        stdout, stderr = p.communicate()
        return stdout.decode('utf-8')

    def fetch_secret(self, secret_name):
        script_path = os.path.join(os.path.dirname(__file__), "fetch_secret.sh")
        raw_response = self.run_cmd(f'{script_path} {secret_name}')
        try:
            secret = json.loads(raw_response)
        except json.decoder.JSONDecodeError:
            self.missing_secrets.append(secret_name)
            return
        if not (('installed' not in secret) or ('client_email' not in secret)) and (self.stage in self.stages):
            self.missing_secrets.append(secret_name)
            return
        return secret

    def fetch_terraform_output(self, output_name, output_infra_dir):
        """See: https://www.terraform.io/docs/commands/output.html"""
        output_infra_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'infra', output_infra_dir))

        # populate infra's vars for the current stage
        self.run_cmd(cmd=f'make -C infra')
        self.run_cmd(cmd=f'terraform refresh', cwd=output_infra_dir)

        # query terraform as to what the needed var is and return it
        terraform_output = self.run_cmd(cmd=f'terraform output {output_name}', cwd=output_infra_dir)
        return terraform_output.strip()

    def check(self, current, expected, secret):
        if current not in expected:
            self.incomplete_secrets.append({'secret': secret,
                                            'current': current,
                                            'expected': expected})

    def run(self):
        # do not check user-custom deploys or prod
        if self.stage in self.stages:
            self.check(self.app_secret['installed']['auth_uri'], self.auth_uri, secret=self.app_secret_name)
            self.check(self.app_secret['installed']['token_uri'], self.token_uri, secret=self.app_secret_name)
            self.check(self.gcp_cred_secret['type'], self.type, secret=self.gcp_cred_secret_name)
            self.check(self.gcp_cred_secret['project_id'], self.project, secret=self.gcp_cred_secret_name)
            self.check(self.gcp_cred_secret['client_email'], self.email, secret=self.gcp_cred_secret_name)

        if self.missing_secrets or self.incomplete_secrets:
            for s in self.incomplete_secrets:
                self.error_message += f'\n' \
                                      f'The following secret                : {s["secret"]}\n' \
                                      f'Had the unexpected setting          : {s["current"]}\n' \
                                      f'When one of these items was expected: {s["expected"]}\n'
            self.error_message += '\n'
            for s in self.missing_secrets:
                self.error_message += f'The following secret was missing    : {s}\n'
            raise ValueError(self.error_message)


def main(stage=None):
    if sys.argv:
        stage = sys.argv[0]
    elif not stage:
        stage = os.environ['DSS_DEPLOYMENT_STAGE']
    s = SecretsChecker(stage)
    s.run()


if __name__ == '__main__':
    main()
