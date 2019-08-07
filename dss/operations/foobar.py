"""
Get/set secret variable values from the AWS Secrets Manager
"""
import os
import sys
import click
import boto3
import select
import typing
import argparse
import json
import logging
from pprint import pprint

from dss.operations import dispatch
from dss.util.aws import ARN as arn
from dss.util.aws.clients import secretsmanager # type: ignore


logger = logging.getLogger(__name__)


events = dispatch.target("foobar",
                         arguments={},
                         help=__doc__)
@events.action("plink",
        arguments={})
def plink(argv: typing.List[str], args: argparse.Namespace):
    """Plink something"""
    store_name = os.environ['DSS_SECRETS_STORE']
    stage_name = os.environ['DSS_DEPLOYMENT_STAGE']

    tiny_secret_name = "es_source_ip-q2baD1"
    short_secret_name = "dcp/dss/dev/es_source_ip-q2baD1"
    full_secret_name = "arn:aws:secretsmanager:us-east-1:861229788715:secret:dcp/dss/dev/es_source_ip-q2baD1"

    try:
        print("------------------")
        print("Full secret name:")
        print(full_secret_name)
        response1 = secretsmanager.get_secret_value(SecretId=full_secret_name)
        pprint(response1)
    except secretsmanager.exceptions.ResourceNotFoundException:
        print(f"No resource with the name {full_secret_name} exists")

    try:
        print("------------------")
        print("Short secret name:")
        print(short_secret_name)
        response2 = secretsmanager.get_secret_value(SecretId=short_secret_name)
        pprint(response2)
    except secretsmanager.exceptions.ResourceNotFoundException:
        print(f"No resource with the name {short_secret_name} exists")

