import binascii
import datetime
import hashlib
import io
import json
import pyrfc3339
import re
import requests
import uuid

from flask import jsonify, make_response, redirect, request
from werkzeug.exceptions import BadRequest

from ..blobstore import BlobNotFoundError
from ..config import Config
from ..hcablobstore import FileMetadata


def head(uuid: str, replica: str=None, timestamp: str=None):
    # NOTE: THIS IS NEVER ACTUALLY CALLED DUE TO A BUG IN CONNEXION.
    # HEAD requests always calls the same endpoint as get, even if we tell it to
    # go to a different method.  However, connexion freaks out if:
    # 1) there is no head() function defined in code.  *or*
    # 2) we tell the head() function to hit the same method using operationId.
    #
    # So in short, do not expect that this function actually gets called.  This
    # is only here to keep connexion from freaking out.
    return get(uuid, replica, timestamp)


def get(uuid: str, replica: str=None, timestamp: str=None):
    if request.method == "GET" and replica is None:
        # replica must be set when it's a GET request.
        raise BadRequest()

    # if it's a HEAD, we can just default to AWS for now.
    # TODO: (ttung) once we can run the endpoints from each cloud, we should
    # just default to the local cloud.
    if request.method == "HEAD" and replica is None:
        replica = "AWS"

    handle, hca_handle, bucket = \
        Config.get_cloud_specific_handles(replica)

    if timestamp is None:
        # list the files and find the one that is the most recent.
        prefix = "files/{}.".format(uuid)
        for matching_file in handle.list(bucket, prefix):
            matching_file = matching_file[len(prefix):]
            if timestamp is None or matching_file > timestamp:
                timestamp = matching_file

    if timestamp is None:
        # no matches!
        return make_response("Cannot find file!", 404)

    # retrieve the file metadata.
    file_metadata = json.loads(
        handle.get(
            bucket,
            "files/{}.{}".format(uuid, timestamp)
        ).decode("utf-8"))

    blob_path = "blobs/" + ".".join((
        file_metadata[FileMetadata.SHA256],
        file_metadata[FileMetadata.SHA1],
        file_metadata[FileMetadata.S3_ETAG],
        file_metadata[FileMetadata.CRC32C],
    ))

    if request.method == "GET":
        response = redirect(handle.generate_presigned_url(
            bucket,
            blob_path,
            handle.get_blob_method()))
    else:
        response = make_response('', 200)

    headers = response.headers
    headers['X-DSS-BUNDLE-UUID'] = uuid
    headers['X-DSS-CREATOR-UID'] = file_metadata[FileMetadata.CREATOR_UID]
    headers['X-DSS-TIMESTAMP'] = timestamp
    headers['X-DSS-CONTENT-TYPE'] = file_metadata[FileMetadata.CONTENT_TYPE]
    headers['X-DSS-CRC32C'] = file_metadata[FileMetadata.CRC32C]
    headers['X-DSS-S3-ETAG'] = file_metadata[FileMetadata.S3_ETAG]
    headers['X-DSS-SHA1'] = file_metadata[FileMetadata.SHA1]
    headers['X-DSS-SHA256'] = file_metadata[FileMetadata.SHA256]

    return response


def list():
    return dict(files=[dict(uuid=str(uuid.uuid4()), name="", versions=[])])


def put(uuid: str):
    uuid = uuid.lower()

    # get the metadata to find out what the eventual file will be called.
    request_data = request.get_json()

    source_url = request_data['source_url']
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
        handle, hca_handle, dst_bucket = \
            Config.get_cloud_specific_handles("aws")
    else:
        # TODO: (ttung) better error messages pls.
        return (
            make_response("I can't support this source_data schema!"),
            requests.codes.bad_request,
        )

    src_bucket = mobj.group('bucket')
    src_object_name = mobj.group('object_name')

    metadata = handle.get_metadata(src_bucket, src_object_name)

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
    try:
        handle.get_metadata(
            dst_bucket, dst_object_name)
    except BlobNotFoundError:
        hca_handle.copy_blob_from_staging(
            src_bucket, src_object_name,
            dst_bucket, dst_object_name)

    # what's the target object name for the file metadata?
    timestamp = request_data.get(
        'timestamp',
        pyrfc3339.generate(
            datetime.datetime.utcnow(),
            accept_naive=True,
            microseconds=True,
            utc=True))
    metadata_object_name = "files/" + uuid + "." + timestamp

    # if it already exists, then it's a failure.
    try:
        handle.get_metadata(dst_bucket, metadata_object_name)
    except BlobNotFoundError:
        pass
    else:
        # TODO: (ttung) better error messages pls.
        return (
            make_response("file already exists!"),
            requests.codes.conflict
        )

    # build the json document for the file metadata.
    document = json.dumps({
        FileMetadata.VERSION: FileMetadata.FILE_FORMAT_VERSION,
        FileMetadata.BUNDLE_UUID: request_data['bundle_uuid'],
        FileMetadata.CREATOR_UID: request_data['creator_uid'],
        FileMetadata.TIMESTAMP: timestamp,
        FileMetadata.CONTENT_TYPE: metadata['hca-dss-content-type'],
        FileMetadata.CRC32C: metadata['hca-dss-crc32c'],
        FileMetadata.S3_ETAG: metadata['hca-dss-s3_etag'],
        FileMetadata.SHA1: metadata['hca-dss-sha1'],
        FileMetadata.SHA256: metadata['hca-dss-sha256'],
    })

    handle.upload_file_handle(
        dst_bucket,
        metadata_object_name,
        io.BytesIO(document.encode("utf-8")))

    return jsonify(
        dict(timestamp=timestamp)), requests.codes.created
