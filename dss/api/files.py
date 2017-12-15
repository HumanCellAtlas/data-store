import datetime
import io
import json
import re
import typing
from enum import Enum, auto
from uuid import uuid4

import iso8601
import requests
from cloud_blobstore import BlobAlreadyExistsError, BlobNotFoundError, BlobStore
from flask import jsonify, make_response, redirect, request
from ..util.version import datetime_to_version_format

from .. import DSSException, dss_handler, stepfunctions
from ..config import Config, Replica
from ..hcablobstore import FileMetadata, HCABlobStore
from ..stepfunctions import s3copyclient
from ..util.aws import AWS_MIN_CHUNK_SIZE


ASYNC_COPY_THRESHOLD = AWS_MIN_CHUNK_SIZE
"""This is the maximum file size that we will copy synchronously."""


@dss_handler
def head(uuid: str, replica: str, version: str=None):
    return get_helper(uuid, Replica[replica], version)


@dss_handler
def get(uuid: str, replica: str, version: str=None):
    return get_helper(uuid, Replica[replica], version)


def get_helper(uuid: str, replica: Replica, version: str=None):
    handle, hca_handle, bucket = Config.get_cloud_specific_handles(replica)

    if version is None:
        # list the files and find the one that is the most recent.
        prefix = "files/{}.".format(uuid)
        for matching_file in handle.list(bucket, prefix):
            matching_file = matching_file[len(prefix):]
            if version is None or matching_file > version:
                version = matching_file

    if version is None:
        # no matches!
        raise DSSException(404, "not_found", "Cannot find file!")

    # retrieve the file metadata.
    try:
        file_metadata = json.loads(
            handle.get(
                bucket,
                "files/{}.{}".format(uuid, version)
            ).decode("utf-8"))
    except BlobNotFoundError as ex:
        raise DSSException(404, "not_found", "Cannot find file!")

    blob_path = "blobs/" + ".".join((
        file_metadata[FileMetadata.SHA256],
        file_metadata[FileMetadata.SHA1],
        file_metadata[FileMetadata.S3_ETAG],
        file_metadata[FileMetadata.CRC32C],
    ))

    if request.method == "GET":
        response = redirect(handle.generate_presigned_GET_url(
            bucket,
            blob_path))
    else:
        response = make_response('', 200)

    headers = response.headers
    headers['X-DSS-BUNDLE-UUID'] = file_metadata[FileMetadata.BUNDLE_UUID]
    headers['X-DSS-CREATOR-UID'] = file_metadata[FileMetadata.CREATOR_UID]
    headers['X-DSS-VERSION'] = version
    headers['X-DSS-CONTENT-TYPE'] = file_metadata[FileMetadata.CONTENT_TYPE]
    headers['X-DSS-SIZE'] = file_metadata[FileMetadata.SIZE]
    headers['X-DSS-CRC32C'] = file_metadata[FileMetadata.CRC32C]
    headers['X-DSS-S3-ETAG'] = file_metadata[FileMetadata.S3_ETAG]
    headers['X-DSS-SHA1'] = file_metadata[FileMetadata.SHA1]
    headers['X-DSS-SHA256'] = file_metadata[FileMetadata.SHA256]

    return response


@dss_handler
def put(uuid: str, json_request_body: dict, version: str=None):
    class CopyMode(Enum):
        NO_COPY = auto()
        COPY_INLINE = auto()
        COPY_ASYNC = auto()

    uuid = uuid.lower()
    if version is not None:
        # convert it to date-time so we can format exactly as the system requires (with microsecond precision)
        timestamp = iso8601.parse_date(version)
    else:
        timestamp = datetime.datetime.utcnow()
    version = datetime_to_version_format(timestamp)

    source_url = json_request_body['source_url']
    cre = re.compile(
        "^"
        "(?P<schema>(?:s3|gs|wasb))"
        "://"
        "(?P<bucket>[^/]+)"
        "/"
        "(?P<key>.+)"
        "$")
    mobj = cre.match(source_url)
    if mobj and mobj.group('schema') == "s3":
        replica = Replica.aws
    elif mobj and mobj.group('schema') == "gs":
        replica = Replica.gcp
    else:
        schema = mobj.group('schema')
        raise DSSException(
            requests.codes.bad_request,
            "unknown_source_schema",
            f"source_url schema {schema} not supported")

    handle, hca_handle, dst_bucket = Config.get_cloud_specific_handles(replica)

    src_bucket = mobj.group('bucket')
    src_key = mobj.group('key')

    metadata = handle.get_user_metadata(src_bucket, src_key)
    size = handle.get_size(src_bucket, src_key)
    content_type = handle.get_content_type(src_bucket, src_key)

    # format all the checksums so they're lower-case.
    for metadata_spec in HCABlobStore.MANDATORY_METADATA.values():
        if metadata_spec['downcase']:
            keyname = typing.cast(str, metadata_spec['keyname'])
            metadata[keyname] = metadata[keyname].lower()

    # what's the target object name for the actual data?
    dst_key = ("blobs/" + ".".join(
        (
            metadata['hca-dss-sha256'],
            metadata['hca-dss-sha1'],
            metadata['hca-dss-s3_etag'],
            metadata['hca-dss-crc32c'],
        )
    )).lower()

    # does it exist? if so, we can skip the copy part.
    copy_mode = CopyMode.COPY_INLINE
    try:
        if hca_handle.verify_blob_checksum(dst_bucket, dst_key, metadata):
            copy_mode = CopyMode.NO_COPY
    except BlobNotFoundError:
        pass

    # build the json document for the file metadata.
    file_metadata = {
        FileMetadata.FORMAT: FileMetadata.FILE_FORMAT_VERSION,
        FileMetadata.BUNDLE_UUID: json_request_body['bundle_uuid'],
        FileMetadata.CREATOR_UID: json_request_body['creator_uid'],
        FileMetadata.VERSION: version,
        FileMetadata.CONTENT_TYPE: content_type,
        FileMetadata.SIZE: size,
        FileMetadata.CRC32C: metadata['hca-dss-crc32c'],
        FileMetadata.S3_ETAG: metadata['hca-dss-s3_etag'],
        FileMetadata.SHA1: metadata['hca-dss-sha1'],
        FileMetadata.SHA256: metadata['hca-dss-sha256'],
    }
    file_metadata_json = json.dumps(file_metadata)

    if copy_mode != CopyMode.NO_COPY and replica == Replica.aws:
        if size > ASYNC_COPY_THRESHOLD:
            copy_mode = CopyMode.COPY_ASYNC

    if copy_mode == CopyMode.COPY_ASYNC:
        state = s3copyclient.copy_write_metadata_sfn_event(
            src_bucket, src_key,
            dst_bucket, dst_key,
            uuid, version,
            file_metadata_json,
        )
        execution_id = str(uuid4())
        stepfunctions.step_functions_invoke("dss-s3-copy-write-metadata-sfn-{stage}", execution_id, state)

        return jsonify(dict(task_id=execution_id, version=version)), requests.codes.accepted
    elif copy_mode == CopyMode.COPY_INLINE:
        handle.copy(src_bucket, src_key, dst_bucket, dst_key)

        # verify the copy was done correctly.
        assert hca_handle.verify_blob_checksum(dst_bucket, dst_key, metadata)

    try:
        write_file_metadata(handle, dst_bucket, uuid, version, file_metadata_json)
        status_code = requests.codes.created
    except BlobAlreadyExistsError:
        # fetch the file metadata, compare it to what we have.
        existing_file_metadata = json.loads(
            handle.get(
                dst_bucket,
                "files/{}.{}".format(uuid, version)
            ).decode("utf-8"))
        if existing_file_metadata != file_metadata:
            raise DSSException(
                requests.codes.conflict,
                "file_already_exists",
                f"file with UUID {uuid} and version {version} already exists")
        status_code = requests.codes.ok

    return jsonify(
        dict(version=version)), status_code


def write_file_metadata(
        handle: BlobStore,
        dst_bucket: str,
        file_uuid: str,
        file_version: str,
        document: str):
    # what's the target object name for the file metadata?
    metadata_key = f"files/{file_uuid}.{file_version}"

    # if it already exists, then it's a failure.
    try:
        handle.get_user_metadata(dst_bucket, metadata_key)
    except BlobNotFoundError:
        pass
    else:
        raise BlobAlreadyExistsError()

    handle.upload_file_handle(
        dst_bucket,
        metadata_key,
        io.BytesIO(document.encode("utf-8")))
