#!/usr/bin/env python
# coding: utf-8
import json

import boto3
import datetime
import os
import sys
import time
import uuid

import argparse


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'sns')))  # noqa
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))  # noqa

from dss.stepfunctions import SFN_TEMPLATE_KEY, SFN_EXECUTION_KEY, SFN_INPUT_KEY
sns = boto3.client('sns')


if __name__ == "__main__":
    for i in range(1100):
        message = {
            SFN_TEMPLATE_KEY: 'dss-TestWait-{stage}',
            SFN_EXECUTION_KEY: str(uuid.uuid4()),
            SFN_INPUT_KEY: json.dumps({})
        }

        publish_args = {
            'Message': json.dumps(message),
            'TopicArn': 'arn:aws:sns:us-east-1:861229788715:dss-sfn-roman'
        }
        sns.publish(**publish_args)
