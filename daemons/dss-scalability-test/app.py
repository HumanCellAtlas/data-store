import random
import tempfile
import os
import uuid
import datetime
import requests
import sys
import logging


from awscli.customizations.s3uploader import S3Uploader
import domovoi

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.util.aws import AWS_MIN_CHUNK_SIZE

app = domovoi.Domovoi()
app.log.setLevel(logging.DEBUG)

file_keys = []
# TODO (rkisin): make bucket name configurable
test_bucket = "org-humancellatlas-dss-test"
replica = "aws"

@app.sns_topic_subscriber("dss-scalability-init-" + os.environ["DSS_DEPLOYMENT_STAGE"])
def init(event, context):
    app.log.info("DSS scalability test  daemon received init event.")

    tempdir = tempfile.gettempdir()
    test_files = event["test_file_keys"]
    test_large_files = event["test_large_file_keys"]

    create_test_files(AWS_MIN_CHUNK_SIZE + 1, test_large_files, tempdir)
    create_test_files(1024, test_files, tempdir)

@app.sns_topic_subscriber("dss-scalability-put-file-" + os.environ["DSS_DEPLOYMENT_STAGE"])
def put_file(event, context):
    app.log.info("DSS scalability test  daemon received put file event.")

    test_files = event["test_file_keys"]
    test_large_files = event["test_large_file_keys"]

    scheme = "s3"

    file_uuid = str(uuid.uuid4())
    bundle_uuid = str(uuid.uuid4())
    timestamp = datetime.datetime.utcnow()
    file_version = timestamp.strftime("%Y-%m-%dT%H%M%S.%fZ")
    headers = {'content-type': 'application/json'}

    rand_file_key = random.choice(test_files + test_large_files)
    app.log.info(f"File put file key: {rand_file_key}")

    request_body = {"bundle_uuid": bundle_uuid,
                    "creator_uid": 0,
                    "source_url": f"{scheme}://{test_bucket}/{rand_file_key}"
                    }

    return requests.post(
        f"https://{os.getenv('API_HOST')}/v1/files/{file_uuid}?version={file_version}",
        headers=headers,
        json=request_body,
    ).json()

def create_test_files(self, size: int, file_keys, tempdir):
    app.log.info(f"Creating {count} test files size {size}")
    for key in file_keys:
        src_data = os.urandom(size + 1)
        with tempfile.NamedTemporaryFile(delete=True) as fh:
            fh.write(src_data)
            fh.flush()
            S3Uploader(tempdir, self.test_bucket).checksum_and_upload_file(fh.name, key, "text/plain")
    app.log.info("Uploaded test files")
