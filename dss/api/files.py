from __future__ import absolute_import, division, print_function, unicode_literals

import os, sys, json, time
from datetime import datetime, timedelta

from flask import redirect

import boto3
import google.cloud.storage
from azure.storage.blob import BlockBlobService, BlobPermissions

s3 = boto3.resource("s3")
gcs = google.cloud.storage.Client()
bbs = BlockBlobService(account_name=os.environ.get("AZURE_STORAGE_ACCOUNT_NAME"),
                       account_key=os.environ.get("AZURE_STORAGE_ACCOUNT_KEY"))

def get(uuid, replica):
    bucket, key = "akislyuk-test", "foo"
    if replica == "aws":
        url = s3.meta.client.generate_presigned_url(
            'get_object',
            Params=dict(Bucket=bucket, Key=key),
            ExpiresIn=9000
        )
    elif replica == "gcs":
        blob = gcs.get_bucket(bucket).blob(key)
        url = blob.generate_signed_url(expiration=int(time.time()+9000))
    elif replica == "abs":
        abs_alias = "czi"

        sig = bbs.generate_blob_shared_access_signature(
            bucket,
            key,
            BlobPermissions.WRITE,
            datetime.utcnow() + timedelta(hours=24)
        )
        url = "https://{abs_alias}.blob.core.windows.net/{bucket}/{key}?{sig}"
        url = url.format(abs_alias=abs_alias, bucket=bucket, key=key, sig=sig)
    return redirect(url)

def list():
    return dict(files=[dict(uuid="", name="", versions=[])])

def post():
    pass
