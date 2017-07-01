import os
import sys
import logging
import json
import time
from urllib.parse import unquote
from string import ascii_letters
from concurrent.futures import ThreadPoolExecutor

import boto3
import domovoi

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss.events.handlers.sync import sync_blob, compose_gs_blobs, copy_part, parts_per_worker
from dss.util.aws import ARN, send_sns_msg, clients, resources

app = domovoi.Domovoi()

dss.Config.set_config(dss.BucketStage.NORMAL)

s3_bucket = dss.Config.get_s3_bucket()

@app.s3_event_handler(bucket=s3_bucket, events=["s3:ObjectCreated:*"])
def process_new_syncable_object(event, context):
    app.log.setLevel(logging.DEBUG)
    if event.get("Event") == "s3:TestEvent":
        app.log.info("DSS sync daemon received S3 test event")
    else:
        bucket = resources.s3.Bucket(event['Records'][0]["s3"]["bucket"]["name"])
        obj = bucket.Object(unquote(event['Records'][0]["s3"]["object"]["key"]))
        sync_blob(source_platform="s3", source_key=obj.key, dest_platform="gs", logger=app.log, context=context)

@app.sns_topic_subscriber("dss-gs-composite-upload-ready")
def compose_upload(event, context):
    msg = json.loads(event["Records"][0]["Sns"]["Message"])
    gs = dss.Config.get_cloud_specific_handles("gcp")[0].gcp_client
    gs_bucket = gs.get_bucket(msg["dest_bucket"])
    while True:
        try:
            context.log("Composing, stage 1")
            compose_stage2_blob_names = []
            if msg["total_parts"] > 32:
                for part_id in range(1, msg["total_parts"] + 1, 32):
                    parts_to_compose = range(part_id, min(part_id + 32, msg["total_parts"] + 1))
                    source_blob_names = ["{}.part{}".format(msg["dest_key"], p) for p in parts_to_compose]
                    dest_blob_name = "{}.part{}".format(msg["dest_key"], ascii_letters[part_id // 32])
                    if gs_bucket.get_blob(dest_blob_name) is None:
                        compose_gs_blobs(gs_bucket, source_blob_names, dest_blob_name)
                    compose_stage2_blob_names.append(dest_blob_name)
            else:
                parts_to_compose = range(1, msg["total_parts"] + 1)
                compose_stage2_blob_names = ["{}.part{}".format(msg["dest_key"], p) for p in parts_to_compose]
            context.log("Composing, stage 2")
            compose_gs_blobs(gs_bucket, compose_stage2_blob_names, msg["dest_key"])
            break
        except AssertionError:
            pass
        time.sleep(5)

@app.sns_topic_subscriber("dss-s3-mpu-ready")
def complete_mpu(event, context):
    msg = json.loads(event["Records"][0]["Sns"]["Message"])
    mpu = resources.s3.Bucket(msg["dest_bucket"]).Object(msg["dest_key"]).MultipartUpload(msg["mpu"])
    while True:
        context.log("Examining parts")
        parts = list(mpu.parts.all())
        if len(parts) == msg["total_parts"]:
            context.log("Closing MPU")
            mpu_parts = [dict(PartNumber=part.part_number, ETag=part.e_tag) for part in parts]
            mpu.complete(MultipartUpload={'Parts': mpu_parts})
            context.log("Closed MPU")
            break
        time.sleep(5)

log_msg = "Copying {source_key}:{part} from {source_platform}://{source_bucket} to {dest_platform}://{dest_bucket}"
platform_to_replica = dict(s3="aws", gs="gcp")
@app.sns_topic_subscriber("dss-copy-parts")
def copy_parts(event, context):
    topic_arn = event["Records"][0]["Sns"]["TopicArn"]
    msg = json.loads(event["Records"][0]["Sns"]["Message"])
    blobstore_handle = dss.Config.get_cloud_specific_handles(platform_to_replica[msg["source_platform"]])[0]
    source_url = blobstore_handle.generate_presigned_GET_url(bucket=msg["source_bucket"], object_name=msg["source_key"])
    futures = []
    gs = dss.Config.get_cloud_specific_handles("gcp")[0].gcp_client
    with ThreadPoolExecutor(max_workers=4) as executor:
        for part in msg["parts"]:
            context.log(log_msg.format(part=part, **msg))
            if msg["dest_platform"] == "s3":
                upload_url = "{host}/{bucket}/{key}?partNumber={part_num}&uploadId={mpu_id}".format(
                    host=clients.s3.meta.endpoint_url,
                    bucket=msg["dest_bucket"],
                    key=msg["dest_key"],
                    part_num=part["id"],
                    mpu_id=msg["mpu"]
                )
            elif msg["dest_platform"] == "gs":
                assert len(msg["parts"]) == 1
                dest_blob_name = "{}.part{}".format(msg["dest_key"], part["id"])
                dest_blob = gs.get_bucket(msg["dest_bucket"]).blob(dest_blob_name)
                upload_url = dest_blob.create_resumable_upload_session(size=part["end"] - part["start"] + 1)
            futures.append(executor.submit(copy_part, upload_url, source_url, msg["dest_platform"], part, context))
    for future in futures:
        future.result()

    closer_sns_topics = dict(s3="dss-s3-mpu-ready", gs="dss-gs-composite-upload-ready")
    if msg["dest_platform"] == "s3":
        mpu = resources.s3.Bucket(msg["dest_bucket"]).Object(msg["dest_key"]).MultipartUpload(msg["mpu"])
        parts = list(mpu.parts.all())
    elif msg["dest_platform"] == "gs":
        part_names = ["{}.part{}".format(msg["dest_key"], p + 1) for p in range(msg["total_parts"])]
        parts = [gs.get_bucket(msg["dest_bucket"]).get_blob(p) for p in part_names]
        parts = [p for p in parts if p is not None]
    context.log("Parts complete: {}".format(len(parts)))
    context.log("Parts outstanding: {}".format(msg["total_parts"] - len(parts)))
    if msg["total_parts"] - len(parts) < parts_per_worker[msg["dest_platform"]] * 2:
        context.log("Calling closer")
        send_sns_msg(ARN(topic_arn, resource=closer_sns_topics[msg["dest_platform"]]), msg)
        context.log("Called closer")
