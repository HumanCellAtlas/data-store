import datetime

import chainedawslambda
import iso8601
from cloud_blobstore import BlobNotFoundError
from enum import Enum, auto

from dss import Config


def copy(uuid: str, replica: str, src_bucket, src_object_name):
    class CopyMode(Enum):
        NO_COPY = auto()
        COPY_INLINE = auto()
        COPY_ASYNC = auto()

    handle, hca_handle, dst_bucket = Config.get_cloud_specific_handles(replica)
    metadata = handle.get_user_metadata(src_bucket, src_object_name)
    size = handle.get_size(src_bucket, src_object_name)

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
    file_metadata = {
        FileMetadata.FORMAT: FileMetadata.FILE_FORMAT_VERSION,
        FileMetadata.BUNDLE_UUID: json_request_body['bundle_uuid'],
        FileMetadata.CREATOR_UID: json_request_body['creator_uid'],
        FileMetadata.VERSION: version,
        FileMetadata.CONTENT_TYPE: metadata['hca-dss-content-type'],
        FileMetadata.SIZE: size,
        FileMetadata.CRC32C: metadata['hca-dss-crc32c'],
        FileMetadata.S3_ETAG: metadata['hca-dss-s3_etag'],
        FileMetadata.SHA1: metadata['hca-dss-sha1'],
        FileMetadata.SHA256: metadata['hca-dss-sha256'],
    }
    file_metadata_json = json.dumps(file_metadata)

    if copy_mode != CopyMode.NO_COPY and replica == "aws":
        if size > ASYNC_COPY_THRESHOLD:
            copy_mode = CopyMode.COPY_ASYNC

    if copy_mode == CopyMode.COPY_ASYNC:
        state = s3copyclient.S3CopyWriteBundleTask.setup_copy_task(
            file_metadata_json,
            uuid,
            version,
            src_bucket, src_object_name,
            dst_bucket, dst_object_name,
            get_s3_chunk_size,
            use_parallel=True,
            timeout_seconds=3600,
        )

        # start a lambda to do the copy.
        task_id = chainedawslambda.aws.schedule_task(s3copyclient.S3CopyWriteBundleTask, state)

        return jsonify(dict(task_id=task_id, version=version)), requests.codes.accepted
    elif copy_mode == CopyMode.COPY_INLINE:
        handle.copy(src_bucket, src_object_name, dst_bucket, dst_object_name)

        # verify the copy was done correctly.
        assert hca_handle.verify_blob_checksum(dst_bucket, dst_object_name, metadata)

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