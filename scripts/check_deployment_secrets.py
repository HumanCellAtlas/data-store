#!/usr/bin/env python
"""
Script to ensure that the secrets in the various HCA stage deployments are not accidentally
changed to personal user credentials (or otherwise).
"""
import subprocess
import os
import json


auth_uri = ['https://auth.data.humancellatlas.org/authorize', 'https://auth.dev.data.humancellatlas.org/authorize']
token_uri = ['https://auth.data.humancellatlas.org/oauth/token','https://auth.dev.data.humancellatlas.org/oauth/token']
dev_email = ['travis-test@human-cell-atlas-travis-test.iam.gserviceaccount.com']
integration_email = ['org-humancellatlas-integration@human-cell-atlas-travis-test.iam.gserviceaccount.com']
staging_email = ['org-humancellatlas-staging@human-cell-atlas-travis-test.iam.gserviceaccount.com']


def fetch_secret(secret_name='application_secrets.json'):
    cmd = ' '.join([os.path.join(os.path.dirname(__file__), 'fetch_secret.sh'), secret_name])
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    secret = json.loads(stdout)
    return secret


def check(current, expected, stage, secret):
    if current not in expected:
        raise ValueError(f'\n\nDeploying to {stage.upper()} could not be completed, because it looks like an AWS secret'
                         f' has an unexpected value.  Please do not change AWS secrets for releases.\n'
                         f'The following secret                : {secret}\n'
                         f'Had the unexpected setting          : {current}\n'
                         f'When one of these items was expected: {expected}\n')


def main(stage=None):
    if not stage:
        stage = os.environ['DSS_DEPLOYMENT_STAGE']

    app_secret_name = 'application_secrets.json'
    gcp_cred_secret_name = 'gcp-credentials.json'
    app_secret = fetch_secret(app_secret_name)
    gcp_cred_secret = fetch_secret(gcp_cred_secret_name)

    # shared checks
    if stage in ('dev', 'integration', 'staging'):
        check(app_secret['installed']['auth_uri'], auth_uri, stage=stage, secret=app_secret_name)
        check(app_secret['installed']['token_uri'], token_uri, stage=stage, secret=app_secret_name)
        check(gcp_cred_secret['type'], ['service_account'], stage=stage, secret=gcp_cred_secret_name)
        check(gcp_cred_secret['project_id'], ['human-cell-atlas-travis-test'], stage=stage, secret=app_secret_name)

    # stage-specific checks
    if stage == 'dev':
        check(gcp_cred_secret['client_email'], dev_email, stage=stage, secret=gcp_cred_secret_name)
    elif stage == 'integration':
        check(gcp_cred_secret['client_email'], integration_email, stage=stage, secret=gcp_cred_secret_name)
    elif stage == 'staging':
        check(gcp_cred_secret['client_email'], staging_email, stage=stage, secret=gcp_cred_secret_name)


if __name__ == '__main__':
    main()
