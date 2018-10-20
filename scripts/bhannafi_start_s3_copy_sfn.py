#!/usr/bin/env python
import os
import sys
import uuid
import json
import boto3
# import argparse

account_id = boto3.client("sts").get_caller_identity()['Account']
s3 = boto3.client("s3")
stepfunctions = boto3.client("stepfunctions")

# parser = argparse.ArgumentParser(description=__doc__)
# parser.add_argument("--secret-name", required=True)
# args = parser.parse_args()

stage = os.environ['DSS_DEPLOYMENT_STAGE']

region = "us-east-1"
sfn_name = f"dss-s3-copy-sfn-{stage}"
state_machine_arn = f"arn:aws:states:{region}:{account_id}:stateMachine:{sfn_name}"
src_bucket = "bhannafi-src"
# src_key = "test"
src_key = "test_wrong_multipart_size"
dst_bucket = "bhannafi-dst"
version = "2018-10-15T035433.801000Z"

def get_metadata(bucket, key):
    response = s3.head_object(
        Bucket=bucket,
        Key=key,
    )
    metadata = response['Metadata'].copy()

    response = s3.get_object_tagging(
        Bucket=bucket,
        Key=key,
    )
    for tag in response['TagSet']:
        key, value = tag['Key'], tag['Value']
        metadata[key] = value

    return metadata

metadata = get_metadata(src_bucket, src_key)

dst_key = ("blobs/" + ".".join(
    (
        metadata['hca-dss-sha256'],
        metadata['hca-dss-sha1'],
        metadata['hca-dss-s3_etag'],
        metadata['hca-dss-crc32c'],
    )
)).lower()

state = {
    "srcbucket": src_bucket,
    "srckey": src_key,
    "dstbucket": dst_bucket,
    "dstkey": dst_key,
    "fileuuid": "80b1143e-d0f7-11e8-8f04-8c8590536ede",
    "fileversion": version,
    "metadata": json.dumps(metadata),
}

resp = stepfunctions.start_execution(
    stateMachineArn=state_machine_arn,
    name=str(uuid.uuid1()),
    input=json.dumps(state),
)

print(resp)
