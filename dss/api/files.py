import json
import re
import time
import typing
from enum import Enum, auto
from uuid import uuid4

import iso8601
import requests
from cloud_blobstore import BlobAlreadyExistsError, BlobNotFoundError
from dcplib.s3_multipart import AWS_MIN_CHUNK_SIZE
from flask import jsonify, make_response, redirect, request

from dss import DSSException, dss_handler, stepfunctions
from dss.config import Config, Replica
from dss.storage.checkout import CheckoutTokenKeys
from dss.storage.checkout.file import get_dst_key, start_file_checkout
from dss.storage.files import write_file_metadata
from dss.storage.hcablobstore import FileMetadata, HCABlobStore, compose_blob_key
from dss.stepfunctions import gscopyclient, s3copyclient
from dss.util import tracing, UrlBuilder
from dss.util.version import datetime_to_version_format


ASYNC_COPY_THRESHOLD = AWS_MIN_CHUNK_SIZE
"""This is the maximum file size that we will copy synchronously."""

"""The retry-after interval in seconds. Sets up downstream libraries / users to
retry request after the specified interval."""
RETRY_AFTER_INTERVAL = 10


@dss_handler
def head(uuid: str, replica: str, version: str=None, token: str=None):
    return get_helper(uuid, Replica[replica], version, token)


@dss_handler
def get(uuid: str, replica: str, version: str=None, token: str=None):
    return get_helper(uuid, Replica[replica], version, token)


def get_helper(uuid: str, replica: Replica, version: str=None, token: str=None):
    with tracing.Subsegment('parameterization'):
        handle = Config.get_blobstore_handle(replica)
        bucket = replica.bucket

    if version is None:
        with tracing.Subsegment('find_latest_version'):
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
        with tracing.Subsegment('load_file'):
            file_metadata = json.loads(
                handle.get(
                    bucket,
                    "files/{}.{}".format(uuid, version)
                ).decode("utf-8"))
    except BlobNotFoundError as ex:
        raise DSSException(404, "not_found", "Cannot find file!")

    with tracing.Subsegment('make_path'):
        blob_path = compose_blob_key(file_metadata)

    if request.method == "GET":
        token, ready = _verify_checkout(replica, token, file_metadata, blob_path)

        if ready:
            response = redirect(handle.generate_presigned_GET_url(
                replica.checkout_bucket,
                get_dst_key(blob_path)))
        else:
            with tracing.Subsegment('make_retry'):
                builder = UrlBuilder(request.url)
                builder.replace_query("token", token)
                response = redirect(str(builder), code=301)
                headers = response.headers
                headers['Retry-After'] = RETRY_AFTER_INTERVAL
                return response

    else:
        response = make_response('', 200)

    with tracing.Subsegment('set_headers'):
        headers = response.headers
        headers['X-DSS-CREATOR-UID'] = file_metadata[FileMetadata.CREATOR_UID]
        headers['X-DSS-VERSION'] = version
        headers['X-DSS-CONTENT-TYPE'] = file_metadata[FileMetadata.CONTENT_TYPE]
        headers['X-DSS-SIZE'] = file_metadata[FileMetadata.SIZE]
        headers['X-DSS-CRC32C'] = file_metadata[FileMetadata.CRC32C]
        headers['X-DSS-S3-ETAG'] = file_metadata[FileMetadata.S3_ETAG]
        headers['X-DSS-SHA1'] = file_metadata[FileMetadata.SHA1]
        headers['X-DSS-SHA256'] = file_metadata[FileMetadata.SHA256]

    return response


def _verify_checkout(
        replica: Replica, token: typing.Optional[str], file_metadata: dict, blob_path: str,
) -> typing.Tuple[str, bool]:
    decoded_token: dict
    if token is None:
        execution_id = start_file_checkout(replica, blob_path)
        start_time = time.time()
        attempts = 0

        decoded_token = {
            CheckoutTokenKeys.EXECUTION_ID: execution_id,
            CheckoutTokenKeys.START_TIME: start_time,
            CheckoutTokenKeys.ATTEMPTS: attempts
        }
    else:
        try:
            decoded_token = json.loads(token)
            decoded_token[CheckoutTokenKeys.ATTEMPTS] += 1
        except (KeyError, ValueError) as ex:
            raise DSSException(requests.codes.bad_request, "illegal_token", "Could not understand token", ex)

    hcablobstore = Config.get_hcablobstore_handle(replica)
    encoded_token = json.dumps(decoded_token)
    try:
        if hcablobstore.verify_blob_checksum_from_dss_metadata(
                replica.checkout_bucket, get_dst_key(blob_path), file_metadata):
            return encoded_token, True
    except BlobNotFoundError:
        pass

    return encoded_token, False


@dss_handler
def put(uuid: str, json_request_body: dict, version: str):
    class CopyMode(Enum):
        NO_COPY = auto()
        COPY_INLINE = auto()
        COPY_ASYNC = auto()

    uuid = uuid.lower()

    # convert it to date-time so we can format exactly as the system requires (with microsecond precision)
    try:
        timestamp = iso8601.parse_date(version)
    except iso8601.ParseError:
        raise DSSException(
            requests.codes.bad_request,
            "illegal_version",
            f"version should be an rfc3339-compliant timestamp")

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

    handle = Config.get_blobstore_handle(replica)
    hca_handle = Config.get_hcablobstore_handle(replica)
    dst_bucket = replica.bucket

    src_bucket = mobj.group('bucket')
    src_key = mobj.group('key')

    metadata = handle.get_user_metadata(src_bucket, src_key)
    size = handle.get_size(src_bucket, src_key)
    content_type = handle.get_content_type(src_bucket, src_key)

    # format all the checksums so they're lower-case.
    for metadata_spec in HCABlobStore.MANDATORY_STAGING_METADATA.values():
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
        if hca_handle.verify_blob_checksum_from_staging_metadata(dst_bucket, dst_key, metadata):
            copy_mode = CopyMode.NO_COPY
    except BlobNotFoundError:
        pass

    # build the json document for the file metadata.
    file_metadata = {
        FileMetadata.FORMAT: FileMetadata.FILE_FORMAT_VERSION,
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

    if copy_mode != CopyMode.NO_COPY and size > ASYNC_COPY_THRESHOLD:
            copy_mode = CopyMode.COPY_ASYNC

    if copy_mode == CopyMode.COPY_ASYNC:
        if replica == Replica.aws:
            state = s3copyclient.copy_write_metadata_sfn_event(
                src_bucket, src_key,
                dst_bucket, dst_key,
                uuid, version,
                file_metadata_json,
            )
            state_machine_name_template = "dss-s3-copy-write-metadata-sfn-{stage}"
        elif replica == Replica.gcp:
            state = gscopyclient.copy_write_metadata_sfn_event(
                src_bucket, src_key,
                dst_bucket, dst_key,
                uuid, version,
                file_metadata_json,
            )
            state_machine_name_template = "dss-gs-copy-write-metadata-sfn-{stage}"
        else:
            raise ValueError("Unhandled replica")

        execution_id = str(uuid4())
        stepfunctions.step_functions_invoke(state_machine_name_template, execution_id, state)
        return jsonify(dict(task_id=execution_id, version=version)), requests.codes.accepted
    elif copy_mode == CopyMode.COPY_INLINE:
        handle.copy(src_bucket, src_key, dst_bucket, dst_key)

        # verify the copy was done correctly.
        assert hca_handle.verify_blob_checksum_from_staging_metadata(dst_bucket, dst_key, metadata)

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
