import logging
import time
import math
from contextlib import closing
from string import ascii_letters
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import google.cloud.storage
import os
from collections import namedtuple
from google.resumable_media._upload import get_content_range

from dcplib.s3_multipart import get_s3_multipart_chunk_size

import dss
from dss import Config, Replica
from dss.api.collections import get_json_metadata, verify_collection
from dss.util.aws import resources, clients
from dss.util.streaming import get_pool_manager, S3SigningChunker
from dss.storage.identifiers import (FILE_PREFIX, BUNDLE_PREFIX, COLLECTION_PREFIX, TOMBSTONE_SUFFIX,
                                     FileFQID, BundleFQID, CollectionFQID)
from dss.storage.hcablobstore import BundleFileMetadata, BundleMetadata, compose_blob_key


logger = logging.getLogger(__name__)

presigned_url_lifetime_seconds = 3600
sync_sfn_dep_wait_sleep_seconds = 8
sync_sfn_num_threads = 8
part_size = {"s3": 64 * 1024 * 1024, "gs": 640 * 1024 * 1024}
parts_per_worker = {"s3": 8, "gs": 1}
gs_upload_chunk_size = 1024 * 1024 * 32
http = get_pool_manager()
max_syncable_metadata_size = 50 * 1024 * 1024

sns_topics = dict(copy_parts="dss-copy-parts-" + os.environ["DSS_DEPLOYMENT_STAGE"],
                  closer=dict(s3="dss-s3-mpu-ready-" + os.environ["DSS_DEPLOYMENT_STAGE"],
                              gs="dss-gs-composite-upload-ready-" + os.environ["DSS_DEPLOYMENT_STAGE"]))

BlobLocation = namedtuple("BlobLocation", "platform bucket blob")

def do_oneshot_copy(source_replica: Replica, dest_replica: Replica, source_key: str):
    gs = Config.get_native_handle(Replica.gcp)
    if source_replica == Replica.aws and dest_replica == Replica.gcp:
        s3_bucket = resources.s3.Bucket(source_replica.bucket)  # type: ignore
        gs_bucket = gs.bucket(dest_replica.bucket)
        source = BlobLocation(platform="s3", bucket=s3_bucket, blob=s3_bucket.Object(source_key))
        dest = BlobLocation(platform="gs", bucket=gs_bucket, blob=gs_bucket.blob(source_key))
        sync_s3_to_gs_oneshot(source, dest)
    elif source_replica == Replica.gcp and dest_replica == Replica.aws:
        gs_bucket = gs.bucket(source_replica.bucket)
        s3_bucket = resources.s3.Bucket(dest_replica.bucket)  # type: ignore
        source = BlobLocation(platform="gs", bucket=gs_bucket, blob=gs_bucket.blob(source_key))
        source.blob.reload()
        dest = BlobLocation(platform="s3", bucket=s3_bucket, blob=s3_bucket.Object(source_key))
        sync_gs_to_s3_oneshot(source, dest)
    else:
        raise NotImplementedError()

def sync_s3_to_gs_oneshot(source: BlobLocation, dest: BlobLocation):
    s3_blob_url = clients.s3.generate_presigned_url(  # type: ignore
        ClientMethod='get_object',
        Params=dict(Bucket=source.bucket.name, Key=source.blob.key),
        ExpiresIn=presigned_url_lifetime_seconds
    )
    with closing(http.request("GET", s3_blob_url, preload_content=False)) as fh:
        if 200 != fh.status:
            msg = f"request to s3 presigned url for {source.blob.key} returned status {fh.status}"
            logger.info(msg)
            raise Exception(msg)  # This will trigger SFN retry behaviour through States.TaskFailed
        gs_blob = dest.bucket.blob(source.blob.key, chunk_size=1024 * 1024)
        gs_blob.metadata = source.blob.metadata
        gs_blob.upload_from_file(fh)
        gs_blob.content_type = source.blob.content_type
        gs_blob.patch()

def sync_gs_to_s3_oneshot(source: BlobLocation, dest: BlobLocation):
    expires_timestamp = int(time.time() + presigned_url_lifetime_seconds)
    gs_blob_url = source.blob.generate_signed_url(expiration=expires_timestamp)
    with closing(http.request("GET", gs_blob_url, preload_content=False)) as fh:
        if 200 != fh.status:
            msg = f"request to gs presigned url for {source.blob.name} returned status {fh.status}"
            logger.info(msg)
            raise Exception(msg)  # This will trigger SFN retry behaviour through States.TaskFailed
        dest.blob.upload_fileobj(fh, ExtraArgs=dict(Metadata=source.blob.metadata or {},
                                                    ContentType=source.blob.content_type))

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

def copy_part(upload_url: str, source_url: str, dest_platform: str, part: dict):
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
            # TODO: brianh: is mypy suppression ok?
            gs_transport = google.auth.transport.requests.AuthorizedSession(gs._credentials)  # type: ignore
            for start in range(0, part["end"] - part["start"] + 1, gs_upload_chunk_size):
                chunk = fh.read(gs_upload_chunk_size)
                headers = {"content-range": get_content_range(start, start + len(chunk) - 1, total_bytes=None)}
                res = gs_transport.request("PUT", upload_url, data=chunk, headers=headers)
                assert 200 <= res.status_code < 400
            assert res.status_code == 200
    return res

def initiate_multipart_upload(source_replica: Replica, dest_replica: Replica, source_key: str):
    assert dest_replica == Replica.aws
    s3_bucket = resources.s3.Bucket(dest_replica.bucket)  # type: ignore
    s3_object = s3_bucket.Object(source_key)
    source_blobstore = Config.get_blobstore_handle(source_replica)
    source_metadata = source_blobstore.get_user_metadata(source_replica.bucket, source_key) or {}
    source_content_type = source_blobstore.get_content_type(source_replica.bucket, source_key)
    mpu = s3_object.initiate_multipart_upload(Metadata=source_metadata,
                                              ContentType=source_content_type)
    return mpu.id

def complete_multipart_upload(msg: dict):
    mpu = resources.s3.Bucket(msg["dest_bucket"]).Object(msg["dest_key"]).MultipartUpload(msg["mpu"])  # type: ignore
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

def compose_upload(msg: dict):
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
        source_blob = resources.s3.Bucket(msg["source_bucket"]).Object(msg["source_key"])  # type: ignore
        dest_blob = gs_bucket.get_blob(msg["dest_key"])
        dest_blob.metadata = source_blob.metadata
        dest_blob.content_type = source_blob.content_type
        dest_blob.patch()
    else:
        raise NotImplementedError()

def range_request(url: str, start: int, end: int):
    return http.request("GET", url, preload_content=False, headers=dict(Range=f"bytes={start}-{end}"))

def get_part_size(object_size, dest_replica):
    if dest_replica.storage_schema == "s3":
        return get_s3_multipart_chunk_size(object_size)
    else:
        return part_size["gs"]

def get_sync_work_state(event: dict):
    source_replica = Replica[event["source_replica"]]
    dest_replica = Replica[event["dest_replica"]]
    object_size = int(event["source_obj_metadata"]["size"])
    part_size = get_part_size(object_size, dest_replica)
    return dict(source_platform=source_replica.storage_schema,
                source_bucket=source_replica.bucket,
                source_key=event["source_key"],
                dest_platform=dest_replica.storage_schema,
                dest_bucket=dest_replica.bucket,
                dest_key=event["source_key"],
                mpu=event.get("mpu_id"),
                total_parts=math.ceil(object_size / part_size))

def exists(replica: Replica, key: str):
    if replica == Replica.aws:
        try:
            resources.s3.Bucket(replica.bucket).Object(key).load()  # type: ignore
            return True
        except clients.s3.exceptions.ClientError:  # type: ignore
            return False
    elif replica == Replica.gcp:
        gs = Config.get_native_handle(Replica.gcp)
        gs_bucket = gs.bucket(Config.get_gs_bucket())
        return gs_bucket.blob(key).exists()
    else:
        raise NotImplementedError()

def dependencies_exist(source_replica: Replica, dest_replica: Replica, key: str):
    """
    Given a source replica and manifest key, checks if all dependencies of the corresponding DSS object are present in
    dest_replica:
     - Given a file manifest key, checks if blobs exist in dest_replica.
     - Given a bundle manifest key, checks if file manifests exist in dest_replica.
     - Given a collection key, checks if all collection contents exist in dest_replica.
    Returns true if all dependencies exist in dest_replica, false otherwise.
    """
    source_handle = Config.get_blobstore_handle(source_replica)
    dest_handle = Config.get_blobstore_handle(dest_replica)
    if key.endswith(TOMBSTONE_SUFFIX):
        return True
    elif key.startswith(FILE_PREFIX):
        file_id = FileFQID.from_key(key)
        file_manifest = get_json_metadata(entity_type="file",
                                          uuid=file_id.uuid,
                                          version=file_id.version,
                                          replica=source_replica,
                                          blobstore_handle=source_handle,
                                          max_metadata_size=max_syncable_metadata_size)
        blob_path = compose_blob_key(file_manifest)
        if exists(dest_replica, blob_path):
            return True
    elif key.startswith(BUNDLE_PREFIX):
        # head all file manifests
        bundle_id = BundleFQID.from_key(key)
        bundle_manifest = get_json_metadata(entity_type="bundle",
                                            uuid=bundle_id.uuid,
                                            version=bundle_id.version,
                                            replica=source_replica,
                                            blobstore_handle=source_handle,
                                            max_metadata_size=max_syncable_metadata_size)
        try:
            with ThreadPoolExecutor(max_workers=20) as e:
                futures = list()
                for file in bundle_manifest[BundleMetadata.FILES]:
                    file_uuid = file[BundleFileMetadata.UUID]
                    file_version = file[BundleFileMetadata.VERSION]
                    futures.append(e.submit(get_json_metadata,
                                            entity_type="file",
                                            uuid=file_uuid,
                                            version=file_version,
                                            replica=dest_replica,
                                            blobstore_handle=source_handle,
                                            max_metadata_size=max_syncable_metadata_size))
                for future in as_completed(futures):
                    future.result()
            return True
        except Exception:
            pass
    elif key.startswith(COLLECTION_PREFIX):
        collection_id = CollectionFQID.from_key(key)
        collection_manifest = get_json_metadata(entity_type="collection",
                                                uuid=collection_id.uuid,
                                                version=collection_id.version,
                                                replica=source_replica,
                                                blobstore_handle=source_handle,
                                                max_metadata_size=max_syncable_metadata_size)
        try:
            verify_collection(contents=collection_manifest["contents"],
                              replica=dest_replica,
                              blobstore_handle=dest_handle)
            return True
        except Exception:
            pass
    else:
        raise NotImplementedError("Unknown prefix for key {}".format(key))
    return False
