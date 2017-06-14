import os, sys, json, boto3, domovoi

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))
sys.path.insert(0, pkg_root)

from dss.events.handlers.index import process_new_indexable_object

app = domovoi.Domovoi()

s3_bucket = os.environ.get("DSS_S3_TEST_BUCKET")

app.s3_event_handler(bucket=s3_bucket, events=["s3:ObjectCreated:*"])(process_new_indexable_object)
