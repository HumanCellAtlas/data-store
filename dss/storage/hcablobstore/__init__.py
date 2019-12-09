import typing

from cloud_blobstore import BlobStore


class HCABlobStore:
    """Abstract base class for all HCA-specific logic for dealing with individual clouds."""

    """
    Metadata fields we expect the staging area to set.  Each field points to a metadata spec, which is a dictionary
    consisting of the following fields:

    keyname: the actual keyname that references the data on the object in the staging area.
    downcase: True iff we are required to downcase the field.
    """
    MANDATORY_STAGING_METADATA = dict(
        SHA1=dict(
            keyname="hca-dss-sha1",
            downcase=True),
        CRC32C=dict(
            keyname="hca-dss-crc32c",
            downcase=True),
        SHA256=dict(
            keyname="hca-dss-sha256",
            downcase=True),
        S3_ETAG=dict(
            keyname="hca-dss-s3_etag",
            downcase=True),
    )

    def __init__(self, handle: BlobStore) -> None:
        self.handle = handle

    def verify_blob_checksum_from_staging_metadata(
            self, bucket: str, key: str, metadata: typing.Dict[str, str]) -> bool:
        """
        Given a blob, verify that the checksum on the cloud store matches the checksum in the metadata dictionary.  The
        keys to the metadata dictionary will be the items in ``MANDATORY_METADATA``.  Each cloud-specific implementation
        of ``HCABlobStore`` should extract the correct field and check it against the cloud-provided checksum.
        :param bucket:
        :param key:
        :param metadata:
        :return: True iff the checksum is correct.
        """
        raise NotImplementedError()

    def verify_blob_checksum_from_dss_metadata(
            self, bucket: str, key: str, dss_metadata: typing.Dict[str, str]) -> bool:
        """
        Given a blob, verify that the checksum on the cloud store matches the checksum in the metadata stored in the
        DSS.  Each cloud-specific implementation of ``HCABlobStore`` should extract the correct field and check it
        against the cloud-provided checksum.
        :param bucket:
        :param key:
        :param dss_metadata:
        :return: True iff the checksum is correct.
        """
        raise NotImplementedError()


class FileMetadata:
    FILE_FORMAT_VERSION = "0.0.4"

    FORMAT = "format"
    CREATOR_UID = "creator_uid"
    VERSION = "version"
    CONTENT_TYPE = "content-type"
    SIZE = "size"
    CRC32C = "crc32c"
    S3_ETAG = "s3-etag"
    SHA1 = "sha1"
    SHA256 = "sha256"


class BundleMetadata:
    FILE_FORMAT_VERSION = "0.0.1"

    FORMAT = "format"
    CREATOR_UID = "creator_uid"
    VERSION = "version"
    FILES = "files"
    PROJECT = "project"


class BundleFileMetadata:
    NAME = "name"
    UUID = "uuid"
    VERSION = "version"
    CONTENT_TYPE = "content-type"
    SIZE = "size"
    INDEXED = "indexed"
    CRC32C = "crc32c"
    S3_ETAG = "s3-etag"
    SHA1 = "sha1"
    SHA256 = "sha256"


def compose_blob_key(file_info: typing.Dict[str, str]) -> str:
    """
    Create the key for a blob, given the file metadata.

    :param file_info: This can either be an object that contains the four keys (SHA256, SHA1, S3_ETAG, and CRC32C) in
                      the key_class.
    """
    return "blobs/" + ".".join((
        file_info[FileMetadata.SHA256],
        file_info[FileMetadata.SHA1],
        file_info[FileMetadata.S3_ETAG],
        file_info[FileMetadata.CRC32C]
    ))
