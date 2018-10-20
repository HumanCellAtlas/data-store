#!/usr/bin/env python
import json
import boto3


s3 = boto3.client('s3')


from_bucket = "org-humancellatlas-dss-config-861229788715"
to_bucket = "org-humancellatlas-861229788715-terraform"


for obj in s3.list_objects(Bucket=from_bucket)['Contents']:
    key = obj['Key']
    if not key.startswith("dss"):
        continue
    new_key = "dss/" + key[4:]
    copy_source = {
        'Bucket': from_bucket,
        'Key': key
    }
    s3.copy(copy_source, to_bucket, new_key)
