import typing

from .implementation import CopyWriteMetadataKey, Key, sfn, copy_write_metadata_sfn


def copy_sfn_event(
        source_bucket: str, source_key: str,
        destination_bucket: str, destination_key: str
) -> typing.MutableMapping[str, str]:
    """Returns the initial event object to start the gs-gs copy stepfunction."""
    return {
        Key.SOURCE_BUCKET: source_bucket,
        Key.SOURCE_KEY: source_key,
        Key.DESTINATION_BUCKET: destination_bucket,
        Key.DESTINATION_KEY: destination_key,
    }


def copy_write_metadata_sfn_event(
        source_bucket: str, source_key: str,
        destination_bucket: str, destination_key: str,
        file_uuid: str, file_version: str,
        metadata: str,
) -> typing.MutableMapping[str, str]:
    """
    Returns the initial event object to start the stepfunction that performs a s3-s3 copy and writes the HCA /files
    metadata file.
    """
    base = copy_sfn_event(source_bucket, source_key, destination_bucket, destination_key)
    base[CopyWriteMetadataKey.FILE_UUID] = file_uuid
    base[CopyWriteMetadataKey.FILE_VERSION] = file_version
    base[CopyWriteMetadataKey.METADATA] = metadata

    return base
