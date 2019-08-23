#!/usr/bin/env python
import os
import sys
import json
import boto3
import subprocess

IAM = boto3.client('iam')
STS = boto3.client('sts')

region = os.environ['AWS_DEFAULT_REGION']
username = os.environ['EVENT_RELAY_AWS_USERNAME']
secret_name = os.environ['EVENT_RELAY_AWS_ACCESS_KEY_SECRETS_NAME']
account_id = STS.get_caller_identity().get('Account')
sns_arn = f'arn:aws:sns:{region}:{account_id}:*'
sqs_arn = f'arn:aws:sqs:{region}:{account_id}:*'

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
                'Resource': sns_arn
            }
        ]
    })
)

IAM.put_user_policy(
    UserName=username,
    PolicyName='sqs_sender',
    PolicyDocument=json.dumps({
        'Version': '2012-10-17',
        'Statement': [
            {
                'Action': [
                    'sqs:SendMessage'
                ],
                'Effect': 'Allow',
                'Resource': sqs_arn
            }
        ]
    })
)

aws_relay_user_key_info = IAM.create_access_key(UserName=username)
aws_relay_user_key_info['AccessKey']['CreateDate'] = aws_relay_user_key_info['AccessKey']['CreateDate'].isoformat()
secret_info = {
    'AccessKey': {
        'AccessKeyId': aws_relay_user_key_info['AccessKey']['AccessKeyId'],
        'SecretAccessKey': aws_relay_user_key_info['AccessKey']['SecretAccessKey'],
    }
}
subprocess.run(
    [
        os.path.join(os.path.dirname(__file__), "dss-ops.py"),
        "secrets",
        "set",
        "--secret-name",
        secret_name,
    ],
    input=json.dumps(secret_info).encode("utf-8"),
)
