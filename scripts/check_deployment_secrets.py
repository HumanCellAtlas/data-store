#!/usr/bin/env python
"""
Script to ensure that the secrets in the various HCA stage deployments are not accidentally
changed to personal user credentials (or otherwise).
"""
import subprocess
import os
import json


stage = os.environ['DSS_DEPLOYMENT_STAGE']

dev_auth_uri = 'https://auth.data.humancellatlas.org/authorize'
dev_token_uri = 'https://auth.data.humancellatlas.org/oauth/token'
dev_email = 'travis-test@human-cell-atlas-travis-test.iam.gserviceaccount.com'

integration_auth_uri = 'https://auth.dev.data.humancellatlas.org/authorize'
integration_token_uri = 'https://auth.data.humancellatlas.org/oauth/token'
integration_email = 'org-humancellatlas-integration@human-cell-atlas-travis-test.iam.gserviceaccount.com'

staging_auth_uri = 'https://auth.dev.data.humancellatlas.org/authorize'
staging_token_uri = 'https://auth.data.humancellatlas.org/oauth/token'
staging_email = 'org-humancellatlas-staging@human-cell-atlas-travis-test.iam.gserviceaccount.com'


def fetch_secret(secret_name='application_secrets.json'):
    cmd = ' '.join([os.path.join(os.path.dirname(__file__), 'fetch_secret.sh'), secret_name])
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    secret = json.loads(stdout)
    return secret


def main():
    app_secret = fetch_secret('application_secrets.json')
    gcp_cred_secret = fetch_secret('gcp-credentials.json')

    # shared checks
    assert gcp_cred_secret['type'] == 'service_account'
    assert gcp_cred_secret['project_id'] == 'human-cell-atlas-travis-test'

    # stage-specific checks
    if stage == 'dev':
        assert app_secret['installed']['auth_uri'] == dev_auth_uri
        assert app_secret['installed']['token_uri'] == dev_token_uri
        assert gcp_cred_secret['client_email'] == dev_email
    elif stage == 'integration':
        assert app_secret['installed']['auth_uri'] == integration_auth_uri
        assert app_secret['installed']['token_uri'] == integration_token_uri
        assert gcp_cred_secret['client_email'] == integration_email
    elif stage == 'staging':
        assert app_secret['installed']['auth_uri'] == staging_auth_uri
        assert app_secret['installed']['token_uri'] == staging_token_uri
        assert gcp_cred_secret['client_email'] == staging_email


if __name__ == '__main__':
    main()
