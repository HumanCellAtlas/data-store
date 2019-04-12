#! /usr/bin/env python

import os
import datetime
import boto3
from uuid import uuid4
from io import BytesIO
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor

from cloud_blobstore.s3 import S3BlobStore
from cloud_blobstore.gs import GSBlobStore

from google.cloud.storage import Client as GCPClient
from hca.dss import DSSClient
from dcplib.s3_multipart import MULTIPART_THRESHOLD, get_s3_multipart_chunk_size
from dcplib.checksumming_io import ChecksummingSink

@lru_cache()
def get_dss_client(deployment="dev"):
    if "prod" == deployment:
        swagger_url=f"https://dss.data.humancellatlas.org/v1/swagger.json"
    else:
        swagger_url=f"https://dss.{deployment}.data.humancellatlas.org/v1/swagger.json"
    return DSSClient(swagger_url=swagger_url)

def get_bucket(deployment="dev"):
    if "prod" == deployment:
        return "org-hca-dss-prod"
    else:
        return f"org-humancellatlas-dss-{deployment}"

@lru_cache()
def get_handle(replica):
    if "aws" == replica:
        client = boto3.client("s3")
        return S3BlobStore(client)
    elif "gcp" == replica:
        client = GCPClient.from_service_account_json(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
        return GSBlobStore(client)
    else:
        raise NotImplementedError(f"Replica `{replica}` is not implemented!")

def list_objects(handle, bucket, pfx):
    for key in handle.list(bucket, pfx):
        yield key

def list_chunks(handle, bucket, pfx, chunk_size=1000):
    keys = list()
    for key in list_objects(handle, bucket, pfx):
        if key.endswith("dead"):
            continue
        keys.append(key)
        if chunk_size == len(keys):
            yield keys
            keys = list()
    yield keys

def get_content_chunks(contents_type="file", chunk_size=200):
    items = list()
    for key in list_objects(bucket, f"{contents_type}s"):
        if key.endswith("dead"):
            continue
        _, fqid = key.split("/")
        uuid, version = fqid.split(".", 1)
        items.append({
            "type": contents_type,
            "uuid": uuid,
            "version": version,
        })
        if len(items) == chunk_size:
            yield items
            items = list()
    yield items

def stage_file(handle, bucket, key, size=16):
    assert size < MULTIPART_THRESHOLD
    data = os.urandom(size)
    chunk_size = get_s3_multipart_chunk_size(size)
    with ChecksummingSink(write_chunk_size=chunk_size) as sink:
        sink.write(data)
        sums = sink.get_checksums()
    metadata = {
        'hca-dss-crc32c': sums['crc32c'].lower(),
        'hca-dss-s3_etag': sums['s3_etag'].lower(),
        'hca-dss-sha1': sums['sha1'].lower(),
        'hca-dss-sha256': sums['sha256'].lower(),
    }
    with BytesIO(data) as fh:
        handle.upload_file_handle(
            bucket,
            key,
            fh,
            "application/octet-stream",
            metadata
        )

def put_file(dss_client, source_url, replica="aws"):
    uuid = str(uuid4())
    version = str(datetime.datetime.utcnow().strftime("%Y-%m-%dT%H%M%S.%fZ"))
    resp = dss_client.put_file(
        replica=replica,
        uuid=uuid,
        version=version,
        source_url=source_url,
        creator_uid=123,
    )
    return f"{uuid}.{version}"

def put_many_versions(dss_client, source_url, number_of_version=100, replica="aws"):
    def _put_file(i):
        fqid = put_file(dss_client, source_url, replica)
        print(i, fqid)
    with ThreadPoolExecutor(max_workers=100) as e:
        for i in range(number_of_version):
            e.submit(_put_file, i)

if __name__ == "__main__":
    handle = get_handle("aws")
    dss_client = get_dss_client("dev")
    staging_bucket = "org-hca-dss-test"
    uuid = str(uuid4())
    key = f"bhannafi_test/{uuid}"
    stage_file(handle, staging_bucket, key)
    put_many_versions(dss_client, f"s3://{staging_bucket}/{key}", number_of_version=100000)
