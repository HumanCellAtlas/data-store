#!/usr/bin/env python
"""
"""
import json
import boto3
import requests
import argparse
import urllib.parse

parser = argparse.ArgumentParser()
parser.add_argument("owner")
parser.add_argument("repo")
parser.add_argument("branch")
args = parser.parse_args()

sm = boto3.client("secretsmanager")

gitlab_api = sm.get_secret_value(SecretId="dss-gitlab-api")['SecretString']
gitlab_token = sm.get_secret_value(SecretId="dss-gitlab-token")['SecretString']
slug = urllib.parse.quote_plus(f"{args.owner}/{args.repo}")
r = requests.get(
    f"https://{gitlab_api}/projects/{slug}/pipelines",
    params={"ref": args.branch},
    headers={"Private-Token": gitlab_token},
)
print(json.loads(r.text)[0]['status'])
