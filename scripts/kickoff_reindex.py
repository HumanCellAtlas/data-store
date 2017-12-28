import os
import json
import boto3

data = dict(
    replica='aws',
    bucket=os.environ.get('DSS_S3_BUCKET'),
    number_of_workers=10)

resp = boto3.client('lambda').invoke(
    FunctionName=f"dss-admin-{os.environ['DSS_DEPLOYMENT_STAGE']}",
    InvocationType='RequestResponse',
    Payload=json.dumps(data)
)

resp_data = json.loads(resp['Payload'].read().decode("utf-8"))
print(resp_data)
