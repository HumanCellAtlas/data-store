import os, sys, json, boto3, domovoi

app = domovoi.Domovoi()

s3_bucket = os.environ.get("DSS_S3_TEST_BUCKET")

@app.s3_event_handler(bucket=s3_bucket, events=["s3:ObjectCreated:*"])
def process_new_indexable_object(event, context):
    bucket = boto3.resource("s3").Bucket(event['Records'][0]["s3"]["bucket"]["name"])
    obj = bucket.Object(event['Records'][0]["s3"]["object"]["key"])
    context.log("Got an event from S3, object head: {}".format(obj.get(Range='bytes=0-80')["Body"].read()))
