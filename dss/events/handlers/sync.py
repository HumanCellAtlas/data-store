import os
import sys
import datetime
import json
import time
import hashlib
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
from collections import namedtuple

import boto3
import botocore.session
import urllib3
import google.cloud.storage
from google.resumable_media._upload import get_content_range
from google.cloud.client import ClientWithProject
from google.cloud._http import JSONConnection

from dss import Config
from dss.util.aws import resources, clients, send_sns_msg, ARN
from dss.util.streaming import get_pool_manager, S3SigningChunker
from dss.blobstore.gs import GSBlobStore

presigned_url_lifetime_seconds = 3600
use_gsts = False
gsts_sched_delay_minutes = 2
part_size = {"s3": 64 * 1024 * 1024, "gs": 640 * 1024 * 1024}
parts_per_worker = {"s3": 8, "gs": 1}
gs_upload_chunk_size = 1024 * 1024 * 32
http = get_pool_manager()

sns_topics = dict(copy_parts="dss-copy-parts-" + os.environ["DSS_DEPLOYMENT_STAGE"],
                  closer=dict(s3="dss-s3-mpu-ready-" + os.environ["DSS_DEPLOYMENT_STAGE"],
                              gs="dss-gs-composite-upload-ready-" + os.environ["DSS_DEPLOYMENT_STAGE"]))

BlobLocation = namedtuple("BlobLocation", "platform bucket blob")

class GStorageTransferClient(ClientWithProject):
    SCOPE = ["https://www.googleapis.com/auth/cloud-platform"]

class GStorageTransferConnection(JSONConnection):
    API_BASE_URL = "https://storagetransfer.googleapis.com"
    API_VERSION = "v1"
    API_URL_TEMPLATE = "{api_base_url}/{api_version}{path}"

# TODO akislyuk: access keys used here should be separate role credentials with need-based access
# TODO akislyuk: schedule a lambda to check the status of the job, get it permissions to execute:
#                storagetransfer.transferJobs().get(jobName=gsts_job["name"]).execute()
def sync_s3_to_gcsts(project_id, s3_bucket_name, gs_bucket_name, source_key, logger):
    gsts_client = GStorageTransferClient()
    gsts_conn = GStorageTransferConnection(client=gsts_client)
    now = datetime.datetime.utcnow()
    schedule_at = now + datetime.timedelta(minutes=gsts_sched_delay_minutes)
    schedule_struct = dict(year=schedule_at.year, month=schedule_at.month, day=schedule_at.day)
    gsts_job_def = {
        "description": "hca-dss-{}-{}".format(int(now.timestamp()), hashlib.md5(source_key.encode()).hexdigest()),
        "status": "ENABLED",
        "projectId": project_id,
        "schedule": {
            "scheduleStartDate": schedule_struct,
            "scheduleEndDate": schedule_struct,
            "startTimeOfDay": dict(hours=schedule_at.hour, minutes=schedule_at.minute)
        },
        "transferSpec": {
            "awsS3DataSource": {
                "bucketName": s3_bucket_name,
                "awsAccessKey": {
                    "accessKeyId": botocore.session.get_session().get_credentials().access_key,
                    "secretAccessKey": botocore.session.get_session().get_credentials().secret_key
                }
            },
            "gcsDataSink": {
                "bucketName": gs_bucket_name
            },
            "transferOptions": {
                "overwriteObjectsAlreadyExistingInSink": False,
                "deleteObjectsUniqueInSink": False,
                "deleteObjectsFromSourceAfterTransfer": False,
            },
            "objectConditions": {
                "includePrefixes": [source_key]
            }
        }
    }
    try:
        gsts_job = gsts_conn.api_request("POST", "/transferJobs", data=gsts_job_def)
        logger.info(gsts_job)
    except Exception as e:
        logger.error(f"FIXME: (akislyuk) GSTS job submission failed: {e}")
    # FIXME akislyuk: the service account doesn't have permission to look at the
    # status of the job, even though it has permission to create it.  I
    # couldn't figure out what permission scope to give the principal in the
    # IAM console, and the service definition at
    # https://storagetransfer.googleapis.com/$discovery/rest?version=v1
    # doesn't tell me either.
    # gsts_job = gsts_conn.api_request("GET", "/" + gsts_job["name"])

def sync_s3_to_gs_oneshot(source, dest, logger):
    s3_blob_url = clients.s3.generate_presigned_url(
        ClientMethod='get_object',
        Params=dict(Bucket=source.bucket.name, Key=source.blob.key),
        ExpiresIn=presigned_url_lifetime_seconds
    )
    with closing(http.request("GET", s3_blob_url, preload_content=False)) as fh:
        gs_blob = dest.bucket.blob(source.blob.key, chunk_size=1024 * 1024)
        gs_blob.metadata = source.blob.metadata
        gs_blob.upload_from_file(fh)

def sync_gs_to_s3_oneshot(source, dest, logger):
    expires_timestamp = int(time.time() + presigned_url_lifetime_seconds)
    gs_blob_url = source.blob.generate_signed_url(expiration=expires_timestamp)
    with closing(http.request("GET", gs_blob_url, preload_content=False)) as fh:
        dest.blob.upload_fileobj(fh, ExtraArgs=dict(Metadata=source.blob.metadata or {}))

def dispatch_multipart_sync(source, dest, logger, context):
    parts_for_worker = []
    futures = []
    total_size = source.blob.content_length if source.platform == "s3" else source.blob.size
    all_parts = list(enumerate(range(0, total_size, part_size[dest.platform])))
    mpu = dest.blob.initiate_multipart_upload(Metadata=source.blob.metadata or {}) if dest.platform == "s3" else None

    with ThreadPoolExecutor(max_workers=4) as executor:
        for part_id, part_start in all_parts:
            parts_for_worker.append(dict(id=part_id + 1,
                                         start=part_start,
                                         end=min(total_size - 1, part_start + part_size[dest.platform] - 1),
                                         total_parts=len(all_parts)))
            if len(parts_for_worker) >= parts_per_worker[dest.platform] or part_id == all_parts[-1][0]:
                logger.info("Invoking dss-copy-parts with %s", ", ".join(str(p["id"]) for p in parts_for_worker))
                sns_msg = dict(source_platform=source.platform,
                               source_bucket=source.bucket.name,
                               source_key=source.blob.key if source.platform == "s3" else source.blob.name,
                               dest_platform=dest.platform,
                               dest_bucket=dest.bucket.name,
                               dest_key=dest.blob.key if dest.platform == "s3" else dest.blob.name,
                               mpu=mpu.id if mpu else None,
                               parts=parts_for_worker,
                               total_parts=len(all_parts))
                sns_arn = ARN(context.invoked_function_arn, service="sns", resource=sns_topics["copy_parts"])
                futures.append(executor.submit(send_sns_msg, sns_arn, sns_msg))
                parts_for_worker = []
    for future in futures:
        future.result()

def sync_blob(source_platform, source_key, dest_platform, logger, context):
    gs = Config.get_cloud_specific_handles("gcp")[0].gcp_client
    logger.info(f"Begin transfer of {source_key} from {source_platform} to {dest_platform}")
    gs_bucket, s3_bucket = gs.bucket(Config.get_gs_bucket()), resources.s3.Bucket(Config.get_s3_bucket())
    if source_platform == "s3" and dest_platform == "gs":
        source = BlobLocation(platform=source_platform, bucket=s3_bucket, blob=s3_bucket.Object(source_key))
        dest = BlobLocation(platform=dest_platform, bucket=gs_bucket, blob=gs_bucket.blob(source_key))
    elif source_platform == "gs" and dest_platform == "s3":
        source = BlobLocation(platform=source_platform, bucket=gs_bucket, blob=gs_bucket.blob(source_key))
        dest = BlobLocation(platform=dest_platform, bucket=s3_bucket, blob=s3_bucket.Object(source_key))
    else:
        raise NotImplementedError()

    if source_platform == "s3" and dest_platform == "gs" and use_gsts:
        sync_s3_to_gcsts(gs.project, source.bucket.name, dest.bucket.name, source_key, logger)
    elif source_platform == "s3" and dest_platform == "gs":
        if dest.blob.exists():
            logger.info(f"Key {source_key} already exists in GS")
            return
        elif source.blob.content_length < part_size["s3"]:
            sync_s3_to_gs_oneshot(source, dest, logger)
        else:
            dispatch_multipart_sync(source, dest, logger, context)
    elif source_platform == "gs" and dest_platform == "s3":
        try:
            dest.blob.load()
            logger.info(f"Key {source_key} already exists in S3")
            return
        except clients.s3.exceptions.ClientError as e:
            if e.response["Error"].get("Message") != "Not Found":
                raise
        source.blob.reload()
        if source.blob.size < part_size["s3"]:
            sync_gs_to_s3_oneshot(source, dest, logger)
        else:
            dispatch_multipart_sync(source, dest, logger, context)
    logger.info(f"Completed transfer of {source_key} from {source.bucket} to {dest.bucket}")

def compose_gs_blobs(gs_bucket, blob_names, dest_blob_name, logger):
    blobs = [gs_bucket.get_blob(b) for b in blob_names]
    logger.info("%d of %d blobs found", len([b for b in blobs if b is not None]), len(blob_names))
    assert not any(b is None for b in blobs)
    dest_blob = gs_bucket.blob(dest_blob_name)
    dest_blob.content_type = blobs[0].content_type
    logger.info("Composing blobs %s into %s", blob_names, dest_blob_name)
    dest_blob.compose(blobs)
    for blob in blobs:
        try:
            blob.delete()
        except:
            pass

def copy_part(upload_url, source_url, dest_platform, part, context):
    gs = Config.get_cloud_specific_handles("gcp")[0].gcp_client
    boto3_session = boto3.session.Session()
    with closing(range_request(source_url, part["start"], part["end"])) as fh:
        if dest_platform == "s3":
            chunker = S3SigningChunker(fh,
                                       part["end"] - part["start"] + 1,
                                       boto3_session.get_credentials(),
                                       "s3",
                                       boto3_session.region_name)
            res = http.request("PUT", upload_url,
                               headers=chunker.get_headers("PUT", upload_url),
                               body=chunker,
                               chunked=True,
                               retries=False)
            context.log(f"Part upload result: {res.status}")
            assert 200 <= res.status < 300
            context.log("Part etag: {}".format(res.headers["ETag"]))
        elif dest_platform == "gs":
            context.log(f"Uploading part {part} to gs")
            gs_transport = google.auth.transport.requests.AuthorizedSession(gs._credentials)
            for start in range(0, part["end"] - part["start"] + 1, gs_upload_chunk_size):
                chunk = fh.read(gs_upload_chunk_size)
                headers = {"content-range": get_content_range(start, start + len(chunk) - 1, total_bytes=None)}
                res = gs_transport.request("PUT", upload_url, data=chunk, headers=headers)
                assert 200 <= res.status_code < 400
            assert res.status_code == 200
    return res

def range_request(url, start, end):
    return http.request("GET", url, preload_content=False, headers=dict(Range=f"bytes={start}-{end}"))
