#!/usr/bin/env python
"""
This recovers gcp credentials from the secrets store
"""

import os
import sys
import boto3

SM = boto3.client('secretsmanager')

gcp_credentials = SM.get_secret_value(
    SecretId='{}/{}/{}'.format(
        os.environ['DSS_SECRETS_STORE'],
        os.environ['DSS_DEPLOYMENT_STAGE'],
        os.environ['GOOGLE_APPLICATION_CREDENTIALS_SECRETS_NAME']
    )
)['SecretString']

application_secrets = SM.get_secret_value(
    SecretId='{}/{}/{}'.format(
        os.environ['DSS_SECRETS_STORE'],
        os.environ['DSS_DEPLOYMENT_STAGE'],
        os.environ['GOOGLE_APPLICATION_SECRETS_SECRETS_NAME']
    )
)['SecretString']

with open(os.environ['GOOGLE_APPLICATION_CREDENTIALS'], 'w') as fp:
    fp.write(gcp_credentials)

with open(os.environ['GOOGLE_APPLICATION_SECRETS'], 'w') as fp:
    fp.write(application_secrets)
