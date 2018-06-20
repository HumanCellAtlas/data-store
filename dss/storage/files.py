import io

from cloud_blobstore import BlobAlreadyExistsError, BlobNotFoundError, BlobStore


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
