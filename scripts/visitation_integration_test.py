#!/usr/bin/env python

import os
import sys
import time
import boto3
from google.cloud.storage import Client

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss.stepfunctions.visitation import handlers
from dss.stepfunctions.visitation.utils import compile_results


def aws_listing(bucket, dirname):
    c = boto3.client('s3')

    resp = c.list_objects_v2(
        Bucket = bucket,
        Prefix = dirname
    )

    if not resp.get('Contents', None):
        return list()

    return [
        obj['Key']
            for obj in resp['Contents']
    ]


def gcp_listing(bucket, dirname):

    c = Client.from_service_account_json(
        os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    )

    resp = c.bucket(bucket).list_blobs(
        prefix = dirname
    )

    return [
        blob.name
            for blob in resp
    ]


def listing(replica, bucket, dirname):
    if 'aws' == replica:
        return aws_listing(bucket, dirname)
    elif 'gcp' == replica:
        return gcp_listing(bucket, dirname)


def run_sentinel(replica, bucket, dirname, k_workers):
    resp = handlers.integration_test(
        replica,
        bucket,
        dirname,
        k_workers
    )

    arn = resp['executionArn']
    name = arn.split(':')[-1]

    while True:
        desc = boto3.client('stepfunctions').describe_execution(
            executionArn = arn
        )

        if 'RUNNING' != desc['status']:
            break

        time.sleep(10)
        print(f'{name} is still running')

    if 'SUCCEEDED' != desc['status']:
        raise Exception(f'visitation sentinel failed {desc}')

    return name


def integration_test(replica, bucket, k_workers=20):

    print(f'Running visitation integration test for {replica}, bucket={bucket}')

    name = run_sentinel(
        replica,
        bucket,
        'files',
        k_workers
    )

    print('Compiling results (this will take a few minutes)')
    results = compile_results(name)
    processed_keys = [key for r in results for key in r['output']['processed_keys']]

    listed_keys = listing(
        replica,
        bucket,
        'files'
    )

    if set(processed_keys) == set(listed_keys):
        print(f'processed {len(processed_keys)} blobs')
        print('passed')
    else:
        print('failed')


integration_test(
    'aws',
    os.environ['DSS_S3_BUCKET_TEST']
)

integration_test(
    'gcp',
    os.environ['DSS_GS_BUCKET_TEST']
)
