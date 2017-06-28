import datetime
import io
import json
import re
import typing
import uuid

import iso8601
import requests

from flask import jsonify, make_response, redirect, request
from werkzeug.exceptions import BadRequest

from ..blobstore import BlobNotFoundError
from ..config import Config
from ..hcablobstore import FileMetadata, HCABlobStore


def head(uuid: str, replica: str=None, version: str=None):
    # NOTE: THIS IS NEVER ACTUALLY CALLED DUE TO A BUG IN CONNEXION.
    # HEAD requests always calls the same endpoint as get, even if we tell it to
    # go to a different method.  However, connexion freaks out if:
    # 1) there is no head() function defined in code.  *or*
    # 2) we tell the head() function to hit the same method using operationId.
    #
    # So in short, do not expect that this function actually gets called.  This
    # is only here to keep connexion from freaking out.
    return get(uuid, replica, version)


def get(uuid: str, replica: str=None, version: str=None):
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

    if version is None:
        # list the files and find the one that is the most recent.
        prefix = "files/{}.".format(uuid)
        for matching_file in handle.list(bucket, prefix):
            matching_file = matching_file[len(prefix):]
            if version is None or matching_file > version:
                version = matching_file

    if version is None:
        # no matches!
        return make_response("Cannot find file!", 404)

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


def list():
    return dict(files=[dict(uuid=str(uuid.uuid4()), name="", versions=[])])


def put(uuid: str, extras: dict, version: str=None):
    uuid = uuid.lower()
    if version is not None:
        # convert it to date-time so we can format exactly as the system requires (with microsecond precision)
        timestamp = iso8601.parse_date(version)
    else:
        timestamp = datetime.datetime.utcnow()
    version = timestamp.strftime("%Y-%m-%dT%H%M%S.%fZ")

    source_url = extras['source_url']
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
    elif mobj and mobj.group('schema') == "gs":
        handle, hca_handle, dst_bucket = \
            Config.get_cloud_specific_handles("gcs")
    else:
        # TODO: (ttung) better error messages pls.
        return (
            make_response("I can't support this source_data schema!"),
            requests.codes.bad_request,
        )

    src_bucket = mobj.group('bucket')
    src_object_name = mobj.group('object_name')

    metadata = handle.get_metadata(src_bucket, src_object_name)

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
    do_copy = True
    try:
        if hca_handle.verify_blob_checksum(dst_bucket, dst_object_name, metadata):
            do_copy = False
    except BlobNotFoundError:
        pass

    if do_copy:
        handle.copy(src_bucket, src_object_name, dst_bucket, dst_object_name)

        # verify the copy was done correctly.
        assert hca_handle.verify_blob_checksum(dst_bucket, dst_object_name, metadata)

    # what's the target object name for the file metadata?
    metadata_object_name = "files/" + uuid + "." + version

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
        FileMetadata.FORMAT: FileMetadata.FILE_FORMAT_VERSION,
        FileMetadata.BUNDLE_UUID: extras['bundle_uuid'],
        FileMetadata.CREATOR_UID: extras['creator_uid'],
        FileMetadata.VERSION: version,
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
        dict(version=version)), requests.codes.created
