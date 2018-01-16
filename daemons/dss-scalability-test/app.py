import random
import tempfile
import os
import uuid
import datetime
import requests
import sys

from awscli.customizations.s3uploader import S3Uploader
import domovoi

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.util.aws import AWS_MIN_CHUNK_SIZE

app = domovoi.Domovoi()

FILE_COUNT = 2
LARGE_FILE_COUNT = 2

file_keys = []
# TODO (rkisin): make bucket name configurable
test_bucket = "org-humancellatlas-dss-test"
replica = "aws"

@app.sns_topic_subscriber("dss-scalability-init")
def init(event, context):
    tempdir = tempfile.gettempdir()
    create_test_files(AWS_MIN_CHUNK_SIZE + 1, LARGE_FILE_COUNT, tempdir)
    create_test_files(1024, FILE_COUNT, tempdir)

@app.sns_topic_subscriber("dss-scalability-put-file")
def put_file(event, context):
    scheme = "s3"

    file_uuid = str(uuid.uuid4())
    bundle_uuid = str(uuid.uuid4())
    timestamp = datetime.datetime.utcnow()
    file_version = timestamp.strftime("%Y-%m-%dT%H%M%S.%fZ")
    headers = {'content-type': 'application/json'}
    rand_file_key = file_keys[random.randint(0, LARGE_FILE_COUNT + FILE_COUNT - 1)]
    print(f"File put file key: {rand_file_key}")

    request_body = {"bundle_uuid": bundle_uuid,
                    "creator_uid": 0,
                    "source_url": f"{scheme}://{test_bucket}/{rand_file_key}"
                    }

    return requests.post(
        f"https://{os.getenv('API_HOST')}/v1/files/{file_uuid}?version={file_version}",
        headers=headers,
        json=request_body,
    ).json()

def create_test_files(self, size: int, count: int, tempdir):
    print(f"Creating {count} test files size {size}")
    for i in range(count):
        test_key = f"dss-scalability-test/{uuid.uuid4()}"
        self.file_keys.append(test_key)
        src_data = os.urandom(size + i)
        with tempfile.NamedTemporaryFile(delete=True) as fh:
            fh.write(src_data)
            fh.flush()
            S3Uploader(tempdir, self.test_bucket).checksum_and_upload_file(fh.name, test_key, "text/plain")
            print(">>> Uploaded file")
    print("done")
