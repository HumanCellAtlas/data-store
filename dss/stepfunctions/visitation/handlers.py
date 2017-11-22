import json
import boto3
from uuid import uuid4
from . import Visitation

def integration_test(replica, bucket, k_workers):

    name = 'integration-test--{}'.format(
        str(uuid4())
    )

    inp = json.dumps({
        'visitation_class_name': 'IntegrationTest',
        'name': name,
        'replica': replica,
        'bucket': bucket,
        'dirname': 'files',
        'k_workers': int(k_workers),
    })

    resp = boto3.client('stepfunctions').start_execution(
        stateMachineArn = Visitation.sentinel_arn,
        name = name,
        input = inp
    )

    return resp
