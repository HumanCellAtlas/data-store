#! /usr/bin/env python

import io
import os
import time

import boto3
from caching_multipart_upload import multipart_parallel_upload

s3 = boto3.client("s3")
bucket = "bhannafi-dst"
uuid = "e16daeac-7b28-4e6d-823a-ae498b2b3439"
key = f"test/{uuid}"

PART_SIZE = 32 * 1024 * 1024

def gen_data(sz):
    d = os.urandom(sz)
    return d

def get():
    resp = s3.get_object(
        Bucket=bucket,
        Key=key
    )
    return resp['Body'].read()

with open("big_blob.dat") as fh:
    data = fh.read()

# for i in range(2):
#     data = gen_data(4000 * 1024 * 1024)
#     with io.BytesIO(data) as fh:
#         multipart_parallel_upload(s3, bucket, key, fh, part_size=PART_SIZE)
#     
#     new_data = gen_data(40 * 1024 * 1024)
#     start_time = time.time()
#     with io.BytesIO(data + new_data) as handle, io.BytesIO(data) as source_handle:
#         if i>=1:
#             part_lookup_object=dict(key=key, handle=source_handle)
#         else:
#             part_lookup_object=None
#         multipart_parallel_upload(
#             s3,
#             bucket,
#             key + "_2",
#             handle,
#             part_lookup_object=part_lookup_object,
#             part_size=PART_SIZE
#         )
#     print(i, "duration:", time.time() - start_time)
