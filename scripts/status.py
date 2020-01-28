#!/usr/bin/env python
"""
This script outputs GitLab pipeline status.
The GitLab API is expected to be stored in AWS secretsmanager with secret id "dcp/dss/gitlab-api"
The GitLab Token is expected to be stored in AWS secretsmanager with secret id "dcp/dss/gitlab-token"
Usage: `scripts/status.py owner repo branch`
Example: `scripts/status.py HumanCellAtlas data-store master`
"""
import os
import json
import boto3
import requests
import argparse
import urllib.parse

parser = argparse.ArgumentParser()
parser.add_argument("owner", help="The group or owner of the repository")
parser.add_argument("repo", help="The repository name")
parser.add_argument("branch", help="Branch to return most recent CI pipeline status")
args = parser.parse_args()

sm = boto3.client("secretsmanager")
parameter_store = os.environ.get("DSS_PARAMETER_STORE")

gitlab_api = sm.get_secret_value(SecretId=f"{parameter_store}/gitlab-api")['SecretString']
gitlab_token = sm.get_secret_value(SecretId=f"{parameter_store}/gitlab-token")['SecretString']
slug = urllib.parse.quote_plus(f"{args.owner}/{args.repo}")
r = requests.get(
    f"https://{gitlab_api}/projects/{slug}/pipelines",
    params={"ref": args.branch},
    headers={"Private-Token": gitlab_token},
)
print(json.loads(r.text)[0]['status'])
