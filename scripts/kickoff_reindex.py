import os
import json
import boto3

data = {
    'replica': 'aws',
    'bucket': os.getenv('DSS_S3_BUCKET'),
    'number_of_workers': 10
}

resp = boto3.client('lambda').invoke(
    FunctionName='dss-backdoor-dev',
    InvocationType='RequestResponse',
    Payload=json.dumps(data)
)

resp_data = json.loads(resp['Payload'].read().decode("utf-8"))
print(resp_data)
