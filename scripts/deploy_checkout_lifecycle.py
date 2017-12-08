#!/usr/bin/env python

import os
import boto3

checkout_bucket = os.environ["DSS_S3_CHECKOUT_BUCKET"]
s3_client = boto3.client('s3')

s3_client.put_bucket_lifecycle_configuration(
    Bucket=checkout_bucket,
    LifecycleConfiguration={
        "Rules": [
            {
            'Filter': {},
            'Status': 'Enabled',
            "Expiration": {
                "Days": 30,
            },
            'ID': "dss_checkout_expiration",
        }]
    }
)
