import os, sys, json, boto3, domovoi
from urllib.parse import unquote

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib')) # noqa
sys.path.insert(0, pkg_root) # noqa

from dss.events.handlers.sync import sync_blob

app = domovoi.Domovoi()

s3_bucket = os.environ.get("DSS_S3_TEST_BUCKET")

@app.s3_event_handler(bucket=s3_bucket, events=["s3:ObjectCreated:*"])
def process_new_syncable_object(event, context):
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(event['Records'][0]["s3"]["bucket"]["name"])
    obj = bucket.Object(unquote(event['Records'][0]["s3"]["object"]["key"]))
    sync_blob(source_platform="s3", source_key=obj.key, dest_platform="gcs", logger=app.log)
