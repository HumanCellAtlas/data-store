import json
import random
import tempfile
import os
import uuid
import datetime

import boto3
import requests
import sys
import logging
import io

import domovoi

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.util.aws import AWS_MIN_CHUNK_SIZE
from dss import Replica, Config

app = domovoi.Domovoi()
app.log.setLevel(logging.DEBUG)

file_keys = []
test_bucket = os.environ["DSS_S3_CHECKOUT_BUCKET"]
replica = Replica.aws
s3_client = boto3.client('s3')

@app.sns_topic_subscriber("dss-scalability-init-" + os.environ["DSS_DEPLOYMENT_STAGE"])
def init_test(event, context):
    app.log.info("DSS scalability test  daemon received init event.")

    print(f"event: {str(event)}")
    msg = json.loads(event["Records"][0]["Sns"]["Message"])

    test_files = msg["test_file_keys"]
    test_large_files = msg["test_large_file_keys"]

    create_test_files(AWS_MIN_CHUNK_SIZE + 1, test_large_files)
    create_test_files(1024, test_files)

@app.sns_topic_subscriber("dss-scalability-put-file-" + os.environ["DSS_DEPLOYMENT_STAGE"])
def put_file(event, context):
    #app.log.info("DSS scalability test  daemon received put file event.")
    msg = json.loads(event["Records"][0]["Sns"]["Message"])

    test_files = msg["test_file_keys"]
    test_large_files = msg["test_large_file_keys"]

    scheme = "s3"

    file_uuid = str(uuid.uuid4())
    bundle_uuid = str(uuid.uuid4())
    timestamp = datetime.datetime.utcnow()
    file_version = timestamp.strftime("%Y-%m-%dT%H%M%S.%fZ")
    headers = {'content-type': 'application/json'}

    rand_file_key = random.choice(test_files + test_large_files)
    #app.log.info(f"File put file key: {rand_file_key}")

    request_body = {"bundle_uuid": bundle_uuid,
                    "creator_uid": 0,
                    "source_url": f"{scheme}://{test_bucket}/{rand_file_key}"
                    }

    return requests.post(
        f"https://{os.getenv('API_HOST')}/v1/files/{file_uuid}?version={file_version}",
        headers=headers,
        json=request_body,
    ).json()

def create_test_files(size: int, file_keys):
    app.log.info(f"Creating {len(file_keys)} test files size {size} in {test_bucket}")
    for key in file_keys:
        src_data = os.urandom(size + 1)
        with tempfile.NamedTemporaryFile(delete=False) as fh:
            fh.write(src_data)
            fh.flush()
            upload(fh.name, test_bucket, key)
        app.log.info(f"Uploaded test file: s3://{test_bucket}/{key}")

def upload(local_path: str, bucket: str, key: str):
    app.log.info("%s", f"Uploading {local_path} to s3://{bucket}/{key}")
    try:
        s3_client.upload_file(local_path, bucket, key)
    except Exception as e:
        app.log.error(f"Unable to upload file: {str(e)}")
