import json
import boto3
from uuid import uuid4
from . import Sentinel

def reindex(replica, bucket, k_workers):

    name = 'reindex--{}'.format(
        str(uuid4())
    )

    arn = Sentinel.ARN

    inp = json.dumps({
        'name': name,
        'replica': replica,
        'bucket' : bucket,
        'k_workers': int(k_workers),
    })

    resp = boto3.client('stepfunctions').start_execution(
        stateMachineArn = Sentinel.ARN,
        name = name,
        input = inp
    )

    return resp
