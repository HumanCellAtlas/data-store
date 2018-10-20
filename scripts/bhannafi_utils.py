import os
import sys
import boto3
import functools
import datetime, pytz
from google.cloud.storage import Client
from cloud_blobstore.s3 import S3BlobStore
from cloud_blobstore.gs import GSBlobStore

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.util.aws import resources, clients


@functools.lru_cache()
def get_lambda_env(stage):
    lambda_env = boto3.client("lambda").get_function_configuration(FunctionName=f"dss-{stage}")
    return lambda_env['Environment']['Variables']


def get_bucket(stage, replica):
    lambda_vars = get_lambda_env(stage)
    if "aws" == replica:
        return lambda_vars['DSS_S3_BUCKET']
    elif "gcp" == replica:
        return lambda_vars['DSS_GS_BUCKET']
    else:
        raise Exception(f"Unknown replica {replica}")


def get_checkout_bucket(stage, replica):
    lambda_vars = get_lambda_env(stage)
    if "aws" == replica:
        return lambda_vars['DSS_S3_CHECKOUT_BUCKET']
    elif "gcp" == replica:
        return lambda_vars['DSS_GS_CHECKOUT_BUCKET']
    else:
        raise Exception(f"Unknown replica {replica}")


@functools.lru_cache()
def get_native_client(replica):
    if "aws" == replica:
        return boto3.client("s3")
    elif "gcp" == replica:
        return Client()
    else:
        raise Exception(f"Unknown replica {replica}")


def get_handle(replica):
    client = get_native_client(replica)
    if "aws" == replica:
        return S3BlobStore(client)
    elif "gcp" == replica:
        return GSBlobStore(client)
    else:
        raise Exception(f"Unknown replica {replica}")


def _age(dt):
    # FIXME
    # assert "UTC" == str(dt.tzinfo)
    # This should be checked, but AWS and GCP encode utc differently
    now = datetime.datetime.now(pytz.UTC)
    age = now - dt
    return age


class Lister:
    def __init__(self, replica):
        self.client = get_native_client(replica)
        self.replica = replica
        if "aws" == replica:
            self.list = self._list_aws
        elif "gcp" == replica:
            self.list = self._list_gcp

    def _list_gcp(self, bucket, prefix, max_age=None):
        kwargs = dict()
        if prefix is not None:
            kwargs['prefix'] = prefix
        bucket_obj = self.client.bucket(bucket)
        for blob_obj in bucket_obj.list_blobs(**kwargs):
            if max_age is not None:
                if _age(blob_obj.time_created) > max_age:
                    continue
            yield blob_obj.name

    def _list_aws(self, bucket, prefix, max_age=None):
        kwargs = dict()
        if prefix is not None:
            kwargs['Prefix'] = prefix
        for item in (
                boto3.resource("s3").Bucket(bucket).
                objects.
                filter(**kwargs)):
            if max_age is not None:
                if _age(item.last_modified) > max_age:
                    continue
            yield item.key


class ReplicatedPair:
    def __init__(self, stage, src_replica, dst_replica):
        self.stage = stage
        self.src_replica = src_replica
        self.dst_replica = dst_replica
        self.src_handle = get_handle(src_replica)
        self.dst_handle = get_handle(dst_replica)
        self.src_bucket = get_bucket(stage, src_replica)
        self.dst_bucket = get_bucket(stage, dst_replica)

    def list_src(self, pfx):
        yield from self.src_handle.list(self.src_bucket, pfx)

    def list_dst(self, pfx):
        yield from self.dst_handle.list(self.dst_bucket, pfx)

    def sizes_match(self, key):
        src_size = self.src_handle.get_size(self.src_bucket, key)
        dst_size = self.dst_handle.get_size(self.dst_bucket, key)
        return src_size == dst_size
