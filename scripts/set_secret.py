#!/usr/bin/env python
import argparse
import os
import select
import sys

import boto3

SM = boto3.client('secretsmanager')
stage = os.environ['FUS_DEPLOYMENT_STAGE']
secrets_store = os.environ['FUS_SECRETS_STORE']

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--secret-name", required=True)
parser.add_argument("--dry-run", required=False, action='store_true')
args = parser.parse_args()

tags = [
    {'Key': 'project', "Value": os.getenv("FUS_PROJECT_TAG", '')},
    {'Key': 'owner', "Value": os.getenv("FUS_OWNER_TAG", '')},
    {'Key': 'env', "Value": os.getenv("FUS_DEPLOYMENT_STAGE")},
    {'Key': 'Name', "Value": args.secret_name},
    {'Key': 'managedBy', "Value": "manual"}
]
secret_id = f'{secrets_store}/{stage}/{args.secret_name}'

if not select.select([sys.stdin, ], [], [], 0.0)[0]:
    print(f"No data in stdin, exiting without setting {secret_id}")
    sys.exit()
val = sys.stdin.read()

print("setting", secret_id)

try:
    resp = SM.describe_secret(SecretId=secret_id)
except SM.exceptions.ResourceNotFoundException:
    if args.dry_run:
        print('Resource Not Found: Creating {}'.format(secret_id))
    else:
        print(f"Creating {secret_id}")
        resp = SM.create_secret(
            Name=secret_id,
            SecretString=val,
            Tags=tags
        )
        print(resp)
else:
    missing_tags = []
    for tag in tags:
        if tag in resp.get('Tags', []):
            pass
        else:
            missing_tags.append(tag)

    if args.dry_run:
        print('Resource Found.')
        print(resp)
        if missing_tags:
            print(f"Missing tags:")
            for tag in missing_tags:
                print(f"\t{tag}")
    else:
        print(f'Resource Found: Updating {secret_id}')
        resp = SM.update_secret(
            SecretId=secret_id,
            SecretString=val
        )
        SM.tag_resource(
            SecretId=secret_id,
            Tags=missing_tags
        )
