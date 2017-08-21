import datetime
import io
import json
import re
import typing
from enum import Enum, auto

import iso8601
import requests

from flask import jsonify, make_response, redirect, request
from werkzeug.exceptions import BadRequest

from .. import DSSException, dss_handler
from ..blobstore import BlobAlreadyExistsError, BlobNotFoundError, BlobStore
from ..config import Config
from ..events.chunkedtask import aws
from ..events.chunkedtask import awscopyclient
from ..hcablobstore import FileMetadata, HCABlobStore
from ..util.aws import get_s3_chunk_size


@dss_handler
def head(uuid: str, version: str=None):
    return get(uuid, None, version)


@dss_handler
def get(uuid: str, replica: str, version: str=None):
    return get_helper(uuid, replica, version)


def get_helper(uuid: str, replica: typing.Optional[str]=None, version: str=None):
    if request.method == "GET" and replica is None:
        # replica must be set when it's a GET request.
        raise BadRequest()

    # if it's a HEAD, we can just default to AWS for now.
    # TODO: (ttung) once we can run the endpoints from each cloud, we should
    # just default to the local cloud.
    if request.method == "HEAD" and replica is None:
        replica = "aws"

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
        return jsonify(dict(
            message="Cannot find file.",
            exception=str(ex),
            HTTPStatusCode=requests.codes.not_found)), requests.codes.not_found

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
    version = timestamp.strftime("%Y-%m-%dT%H%M%S.%fZ")

    source_url = json_request_body['source_url']
    cre = re.compile(
        "^"
        "(?P<schema>(?:s3|gs|wasb))"
        "://"
        "(?P<bucket>[^/]+)"
        "/"
        "(?P<object_name>.+)"
        "$")
    mobj = cre.match(source_url)
    if mobj and mobj.group('schema') == "s3":
        replica = "aws"
    elif mobj and mobj.group('schema') == "gs":
        replica = "gcp"
    else:
        # TODO: (ttung) better error messages pls.
        return (
            make_response("I can't support this source_data schema!"),
            requests.codes.bad_request,
        )
    handle, hca_handle, dst_bucket = Config.get_cloud_specific_handles(replica)

    src_bucket = mobj.group('bucket')
    src_object_name = mobj.group('object_name')

    metadata = handle.get_user_metadata(src_bucket, src_object_name)

    # format all the checksums so they're lower-case.
    for metadata_spec in HCABlobStore.MANDATORY_METADATA.values():
        if metadata_spec['downcase']:
            keyname = typing.cast(str, metadata_spec['keyname'])
            metadata[keyname] = metadata[keyname].lower()

    # what's the target object name for the actual data?
    dst_object_name = ("blobs/" + ".".join(
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
        if hca_handle.verify_blob_checksum(dst_bucket, dst_object_name, metadata):
            copy_mode = CopyMode.NO_COPY
    except BlobNotFoundError:
        pass

    # build the json document for the file metadata.
    document = json.dumps({
        FileMetadata.FORMAT: FileMetadata.FILE_FORMAT_VERSION,
        FileMetadata.BUNDLE_UUID: json_request_body['bundle_uuid'],
        FileMetadata.CREATOR_UID: json_request_body['creator_uid'],
        FileMetadata.VERSION: version,
        FileMetadata.CONTENT_TYPE: metadata['hca-dss-content-type'],
        FileMetadata.CRC32C: metadata['hca-dss-crc32c'],
        FileMetadata.S3_ETAG: metadata['hca-dss-s3_etag'],
        FileMetadata.SHA1: metadata['hca-dss-sha1'],
        FileMetadata.SHA256: metadata['hca-dss-sha256'],
    })

    if copy_mode != CopyMode.NO_COPY and replica == "aws":
        copy_mode = CopyMode.COPY_ASYNC

    if copy_mode == CopyMode.COPY_ASYNC:
        state = awscopyclient.S3CopyTask.setup_copy_task(
            src_bucket, src_object_name,
            dst_bucket, dst_object_name,
            get_s3_chunk_size,
        )
        state[awscopyclient.S3CopyWriteBundleTaskKeys.FILE_UUID] = uuid
        state[awscopyclient.S3CopyWriteBundleTaskKeys.FILE_VERSION] = version
        state[awscopyclient.S3CopyWriteBundleTaskKeys.METADATA] = document

        # start a lambda to do the copy.
        task_id = aws.schedule_task(awscopyclient.AWS_S3_COPY_AND_WRITE_METADATA_CLIENT_NAME, state)

        return jsonify(dict(task_id=task_id, version=version)), requests.codes.accepted
    elif copy_mode == CopyMode.COPY_INLINE:
        handle.copy(src_bucket, src_object_name, dst_bucket, dst_object_name)

        # verify the copy was done correctly.
        assert hca_handle.verify_blob_checksum(dst_bucket, dst_object_name, metadata)

    try:
        write_file_metadata(handle, dst_bucket, uuid, version, document)
    except BlobAlreadyExistsError:
        # TODO: (ttung) better error messages pls.
        return (
            make_response("file already exists!"),
            requests.codes.conflict
        )

    return jsonify(
        dict(version=version)), requests.codes.created


def write_file_metadata(
        handle: BlobStore,
        dst_bucket: str,
        file_uuid: str,
        file_version: str,
        document: str):
    # what's the target object name for the file metadata?
    metadata_object_name = f"files/{file_uuid}.{file_version}"

    # if it already exists, then it's a failure.
    try:
        handle.get_user_metadata(dst_bucket, metadata_object_name)
    except BlobNotFoundError:
        pass
    else:
        raise BlobAlreadyExistsError()

    handle.upload_file_handle(
        dst_bucket,
        metadata_object_name,
        io.BytesIO(document.encode("utf-8")))
