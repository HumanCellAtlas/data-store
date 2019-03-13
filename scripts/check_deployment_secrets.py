#!/usr/bin/env python
"""
Script to ensure that the secrets in the various HCA stage deployments are not accidentally
changed to personal user credentials (or otherwise).

Requires Terraform.
"""
import subprocess
import os
import json


class SecretsChecker(object):
    def __init__(self, stage):
        self.stage = stage
        self.stages = ('dev', 'integration', 'staging')
        self.service_account = self.fetch_terraform_output("service_account", "gcp_service_account").strip()

        self.email = [f'{self.service_account}@human-cell-atlas-travis-test.iam.gserviceaccount.com']
        self.project = ['human-cell-atlas-travis-test']
        self.auth_uri = ['https://auth.data.humancellatlas.org/authorize',
                         'https://auth.dev.data.humancellatlas.org/authorize']
        self.token_uri = ['https://auth.data.humancellatlas.org/oauth/token',
                          'https://auth.dev.data.humancellatlas.org/oauth/token']

        self.app_secret_name = os.environ['GOOGLE_APPLICATION_SECRETS_SECRETS_NAME']
        self.gcp_cred_secret_name = os.environ['GOOGLE_APPLICATION_CREDENTIALS_SECRETS_NAME']
        self.app_secret = self.fetch_secret(self.app_secret_name)
        self.gcp_cred_secret = self.fetch_secret(self.gcp_cred_secret_name)

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
            raise RuntimeError(f'The following secret, {secret_name}, appears to no longer exist or is malformed.')
        if not (('installed' not in secret) or ('client_email' not in secret)) and (self.stage in self.stages):
            raise RuntimeError(f'The following secret, {secret_name}, appears to no longer exist or is malformed.')
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
            raise ValueError(f'\n\nDeploying to {self.stage.upper()} could not be completed.'
                             f'It looks like an AWS secret has an unexpected value.\n'
                             f'Please do not change AWS secrets for releases.\n'
                             f'The following secret                : {secret}\n'
                             f'Had the unexpected setting          : {current}\n'
                             f'When one of these items was expected: {expected}\n')

    def run(self):
        # do not check user-custom deploys or prod
        if self.stage in self.stages:
            self.check(self.app_secret['installed']['auth_uri'], self.auth_uri, secret=self.app_secret_name)
            self.check(self.app_secret['installed']['token_uri'], self.token_uri, secret=self.app_secret_name)
            self.check(self.gcp_cred_secret['type'], ['service_account'], secret=self.gcp_cred_secret_name)
            self.check(self.gcp_cred_secret['project_id'], self.project, secret=self.gcp_cred_secret_name)
            self.check(self.gcp_cred_secret['client_email'], self.email, secret=self.gcp_cred_secret_name)


def main(stage=None):
    if not stage:
        stage = os.environ['DSS_DEPLOYMENT_STAGE']
    s = SecretsChecker(stage)
    s.run()


if __name__ == '__main__':
    main()
