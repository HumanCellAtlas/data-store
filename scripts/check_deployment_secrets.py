#!/usr/bin/env python
"""
Script to ensure that the secrets in an HCA stage deployment are not accidentally
changed to personal user credentials (or otherwise).  Requires Terraform.

Will only check the canoncial HCA stages ('dev', 'integration', 'staging').

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
import copy


class SecretsChecker(object):
    def __init__(self, stage):
        self.stage = stage
        self.stages = {'dev': 'environment',
                       'integration': 'environment.integration',
                       'staging': 'environment.staging'}

        self.missing_secrets = []
        self.malformed_secrets = []
        self.incomplete_secrets = []
        self.error_message = f'\n\n' \
                             f'Deploying to {self.stage.upper()} could not be completed.\n' \
                             f'It looks like an AWS secret has an unexpected value.\n' \
                             f'Please do not change AWS secrets for releases.\n'

        if self.stage not in self.stages:
            print(f'Custom stage "{self.stage}" provided.  Secret checking skipped.')
            return

        self.stage_env = copy.deepcopy(os.environ)
        self.stage_env = self.get_stage_env(self.stages[self.stage])
        self.service_account = self.fetch_terraform_output("service_account", "gcp_service_account").strip()

        self.email = [f'{self.service_account}@human-cell-atlas-travis-test.iam.gserviceaccount.com']
        self.project = ['human-cell-atlas-travis-test']
        self.type = ['service_account']
        self.auth_uri = ['https://auth.data.humancellatlas.org/oauth/authorize',
                         'https://auth.dev.data.humancellatlas.org/oauth/authorize']
        self.token_uri = ['https://auth.data.humancellatlas.org/oauth/token',
                          'https://auth.dev.data.humancellatlas.org/oauth/token']

        self.app_secret_name = os.environ['GOOGLE_APPLICATION_SECRETS_SECRETS_NAME']
        self.gcp_cred_secret_name = os.environ['GOOGLE_APPLICATION_CREDENTIALS_SECRETS_NAME']
        self.app_secret = self.fetch_secret(self.app_secret_name)
        self.gcp_cred_secret = self.fetch_secret(self.gcp_cred_secret_name)

    def run_cmd(self, cmd, cwd=os.getcwd(), shell=True):
        p = subprocess.Popen(cmd,
                             shell=shell,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             cwd=cwd,
                             env=self.stage_env)
        stdout, stderr = p.communicate()
        if p.returncode != 0:
            raise RuntimeError(f'While checking secrets, an error occured:\n'
                               f'stdout: {stdout}\n\n'
                               f'stderr: {stderr}\n')
        return stdout.decode('utf-8')

    def get_stage_env(self, env_file):
        dump = 'python -c "import os, json; print(json.dumps(dict(os.environ)))"'
        cmd = ['bash', '-c', f'source {env_file} && {dump}']
        return json.loads(self.run_cmd(cmd, shell=False))

    def fetch_secret(self, secret_name):
        script_path = os.path.join(os.path.dirname(__file__), "fetch_secret.sh")
        raw_response = self.run_cmd(f'{script_path} {secret_name}')
        try:
            secret = json.loads(raw_response)
        except json.decoder.JSONDecodeError:
            self.missing_secrets.append(secret_name)
            return
        if not (('installed' not in secret) or ('client_email' not in secret)) and (self.stage in self.stages):
            self.malformed_secrets.append(secret_name)
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
        if not terraform_output:
            print(f'Terraform output returned nothing.\n'
                  f'Check your terraform setup by running "terraform output {output_name}" in dir:\n'
                  f'data-store/infra/gcp_service_account\n\n')
        return terraform_output.strip()

    def check(self, current, expected, secret):
        if current not in expected:
            self.incomplete_secrets.append({'secret': secret,
                                            'current': current,
                                            'expected': expected})

    def run(self):
        # do not check user-custom deploys
        if self.stage in self.stages:
            print(f'Now checking the secrets for {self.stage}...')
            self.check(self.app_secret['installed']['auth_uri'], self.auth_uri, secret=self.app_secret_name)
            self.check(self.app_secret['installed']['token_uri'], self.token_uri, secret=self.app_secret_name)
            self.check(self.gcp_cred_secret['type'], self.type, secret=self.gcp_cred_secret_name)
            self.check(self.gcp_cred_secret['project_id'], self.project, secret=self.gcp_cred_secret_name)
            self.check(self.gcp_cred_secret['client_email'], self.email, secret=self.gcp_cred_secret_name)
            print(f'Secret check complete for {self.stage}.')

        if self.missing_secrets or self.incomplete_secrets or self.malformed_secrets:
            for s in self.incomplete_secrets:
                self.error_message += f'\n' \
                                      f'The following secret                : {s["secret"]}\n' \
                                      f'Had the unexpected setting          : {s["current"]}\n' \
                                      f'When one of these items was expected: {s["expected"]}\n'
            self.error_message += '\n'
            for s in self.missing_secrets:
                self.error_message += f'The following secret was missing    : {s}\n'
            self.error_message += '\n'
            for s in self.malformed_secrets:
                self.error_message += f'The following secret seems malformed: {s}\n'
            raise ValueError(self.error_message)


def main(stage=None):
    if len(sys.argv) > 1:
        stage = sys.argv[1]
    elif not stage:
        stage = os.environ['DSS_DEPLOYMENT_STAGE']
    s = SecretsChecker(stage)
    s.run()


if __name__ == '__main__':
    main()
