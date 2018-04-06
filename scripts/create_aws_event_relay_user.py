#!/usr/bin/env python
import os
import sys
import json
import boto3

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.append(pkg_root)

import dss_deployment
active = dss_deployment.active()

IAM = boto3.client('iam')
STS = boto3.client('sts')

region = active.value('AWS_DEFAULT_REGION')
username = active.value('DSS_EVENT_RELAY_AWS_USERNAME')
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
