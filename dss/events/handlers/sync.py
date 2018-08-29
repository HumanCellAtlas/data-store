import datetime
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from string import ascii_letters

import boto3
import botocore.session
import google.cloud.storage
import hashlib
import os
from collections import namedtuple
from google.cloud._http import JSONConnection
from google.cloud.client import ClientWithProject
from google.resumable_media._upload import get_content_range

import dss
from dss import Config, Replica
from dss.util.aws import resources, clients, send_sns_msg, ARN
from dss.util.streaming import get_pool_manager, S3SigningChunker


logger = logging.getLogger(__name__)

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

def sync_s3_to_gs_oneshot(source, dest):
    s3_blob_url = clients.s3.generate_presigned_url(
        ClientMethod='get_object',
        Params=dict(Bucket=source.bucket.name, Key=source.blob.key),
        ExpiresIn=presigned_url_lifetime_seconds
    )
    with closing(http.request("GET", s3_blob_url, preload_content=False)) as fh:
        gs_blob = dest.bucket.blob(source.blob.key, chunk_size=1024 * 1024)
        gs_blob.metadata = source.blob.metadata
        gs_blob.upload_from_file(fh)


def sync_gs_to_s3_oneshot(source, dest):
    expires_timestamp = int(time.time() + presigned_url_lifetime_seconds)
    gs_blob_url = source.blob.generate_signed_url(expiration=expires_timestamp)
    with closing(http.request("GET", gs_blob_url, preload_content=False)) as fh:
        dest.blob.upload_fileobj(fh, ExtraArgs=dict(Metadata=source.blob.metadata or {}))


def dispatch_multipart_sync(source, dest, context):
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


def sync_blob(source_platform, source_key, dest_platform, context):
    gs = Config.get_native_handle(Replica.gcp)
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

    if source_platform == "s3" and dest_platform == "gs":
        if dest.blob.exists():
            logger.info(f"Key {source_key} already exists in GS")
            return
        elif source.blob.content_length < part_size["s3"]:
            sync_s3_to_gs_oneshot(source, dest)
        else:
            dispatch_multipart_sync(source, dest, context)
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
            sync_gs_to_s3_oneshot(source, dest)
        else:
            dispatch_multipart_sync(source, dest, context)
    logger.info(f"Completed transfer of {source_key} from {source.bucket} to {dest.bucket}")


def compose_gs_blobs(gs_bucket, blob_names, dest_blob_name):
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
        except Exception:
            pass


def copy_part(upload_url, source_url, dest_platform, part):
    gs = Config.get_native_handle(Replica.gcp)
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
            logger.info(f"Part upload result: {res.status}")
            assert 200 <= res.status < 300
            logger.info("Part etag: {}".format(res.headers["ETag"]))
        elif dest_platform == "gs":
            logger.info(f"Uploading part {part} to gs")
            gs_transport = google.auth.transport.requests.AuthorizedSession(gs._credentials)
            for start in range(0, part["end"] - part["start"] + 1, gs_upload_chunk_size):
                chunk = fh.read(gs_upload_chunk_size)
                headers = {"content-range": get_content_range(start, start + len(chunk) - 1, total_bytes=None)}
                res = gs_transport.request("PUT", upload_url, data=chunk, headers=headers)
                assert 200 <= res.status_code < 400
            assert res.status_code == 200
    return res


def copy_parts_handler(topic_arn, msg, platform_to_replica):
    log_msg = "Copying {source_key}:{part} from {source_platform}://{source_bucket} to {dest_platform}://{dest_bucket}"
    blobstore_handle = dss.Config.get_blobstore_handle(platform_to_replica[msg["source_platform"]])
    source_url = blobstore_handle.generate_presigned_GET_url(bucket=msg["source_bucket"], key=msg["source_key"])
    futures = []
    gs = dss.Config.get_native_handle(Replica.gcp)
    with ThreadPoolExecutor(max_workers=4) as executor:
        for part in msg["parts"]:
            logger.info(log_msg.format(part=part, **msg))
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
            futures.append(executor.submit(copy_part, upload_url, source_url, msg["dest_platform"], part))
    for future in futures:
        future.result()

    if msg["dest_platform"] == "s3":
        mpu = resources.s3.Bucket(msg["dest_bucket"]).Object(msg["dest_key"]).MultipartUpload(msg["mpu"])
        parts = list(mpu.parts.all())
    elif msg["dest_platform"] == "gs":
        part_names = ["{}.part{}".format(msg["dest_key"], p + 1) for p in range(msg["total_parts"])]
        parts = [gs.get_bucket(msg["dest_bucket"]).get_blob(p) for p in part_names]
        parts = [p for p in parts if p is not None]
    logger.info("Parts complete: {}".format(len(parts)))
    logger.info("Parts outstanding: {}".format(msg["total_parts"] - len(parts)))
    if msg["total_parts"] - len(parts) < parts_per_worker[msg["dest_platform"]] * 2:
        logger.info("Calling closer")
        send_sns_msg(ARN(topic_arn, resource=sns_topics["closer"][msg["dest_platform"]]), msg)
        logger.info("Called closer")


def complete_multipart_upload(msg):
    mpu = resources.s3.Bucket(msg["dest_bucket"]).Object(msg["dest_key"]).MultipartUpload(msg["mpu"])
    while True:
        logger.info("Examining parts")
        parts = list(mpu.parts.all())
        if len(parts) == msg["total_parts"]:
            logger.info("Closing MPU")
            mpu_parts = [dict(PartNumber=part.part_number, ETag=part.e_tag) for part in parts]
            mpu.complete(MultipartUpload={'Parts': mpu_parts})
            logger.info("Closed MPU")
            break
        time.sleep(5)


def compose_upload(msg):
    """
    See https://cloud.google.com/storage/docs/composite-objects for details of the Google Storage API used here.
    """
    gs_max_compose_parts = 32
    gs = dss.Config.get_native_handle(Replica.gcp)
    gs_bucket = gs.get_bucket(msg["dest_bucket"])
    while True:
        try:
            logger.info("Composing, stage 1")
            compose_stage2_blob_names = []
            if msg["total_parts"] > gs_max_compose_parts:
                for part_id in range(1, msg["total_parts"] + 1, gs_max_compose_parts):
                    parts_to_compose = range(part_id, min(part_id + gs_max_compose_parts, msg["total_parts"] + 1))
                    source_blob_names = ["{}.part{}".format(msg["dest_key"], p) for p in parts_to_compose]
                    dest_blob_name = "{}.part{}".format(msg["dest_key"], ascii_letters[part_id // gs_max_compose_parts])
                    if gs_bucket.get_blob(dest_blob_name) is None:
                        compose_gs_blobs(gs_bucket, source_blob_names, dest_blob_name)
                    compose_stage2_blob_names.append(dest_blob_name)
            else:
                parts_to_compose = range(1, msg["total_parts"] + 1)
                compose_stage2_blob_names = ["{}.part{}".format(msg["dest_key"], p) for p in parts_to_compose]
            logger.info("Composing, stage 2")
            compose_gs_blobs(gs_bucket, compose_stage2_blob_names, msg["dest_key"])
            break
        except AssertionError:
            pass
        time.sleep(5)
    if msg["source_platform"] == "s3":
        source_blob = resources.s3.Bucket(msg["source_bucket"]).Object(msg["source_key"])
        dest_blob = gs_bucket.get_blob(msg["dest_key"])
        dest_blob.metadata = source_blob.metadata
    else:
        raise NotImplementedError()


def range_request(url, start, end):
    return http.request("GET", url, preload_content=False, headers=dict(Range=f"bytes={start}-{end}"))
