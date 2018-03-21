#!/usr/bin/env python
import os
import sys
import json
import boto3

IAM = boto3.client('iam')
STS = boto3.client('sts')

region = os.environ['AWS_DEFAULT_REGION']
stage = os.environ['DSS_DEPLOYMENT_STAGE']
username = f'dss-gs-event-relay-{stage}'
account_id = STS.get_caller_identity().get('Account')
resource_arn = f'arn:aws:sns:{region}:{account_id}:*'

try:
    resp = IAM.create_user(
        Path='/',
        UserName=username
    )
except IAM.exceptions.EntityAlreadyExistsException:
    pass

IAM.put_user_policy(
    UserName=username,
    PolicyName='sns_publisher',
    PolicyDocument=json.dumps({
        'Version': '2012-10-17',
        'Statement': [
            {
                'Action': [
                    'sns:Publish'
                ],
                'Effect': 'Allow',
                'Resource': resource_arn
            }
        ]
    })
)
