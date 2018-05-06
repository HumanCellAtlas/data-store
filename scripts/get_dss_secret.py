#!/usr/bin/env python
"""
This script recovers a dss secret from AWS Secret Manager and writes it to a file.
"""

import os
import sys
import boto3
import argparse

SM = boto3.client('secretsmanager')

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("name", help="Name of secret")
parser.add_argument("filepath", help="Name of file where the secret will be written")
args = parser.parse_args()

secret = SM.get_secret_value(
    SecretId='{}/{}/{}'.format(
        os.environ['DSS_SECRETS_STORE'],
        os.environ['DSS_DEPLOYMENT_STAGE'],
        args.name
    )
)['SecretString']

with open(args.filepath, "w") as fp:
    fp.write(secret)
