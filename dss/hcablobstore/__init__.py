import typing


class HCABlobStore:
    """Abstract base class for all HCA-specific logic for dealing with individual clouds."""

    """
    Metadata fields we expect the staging area to set.  Each field points to a metadata spec, which is a dictionary
    consisting of the following fields:

    keyname: the actual keyname that references the data on the object in the staging area.
    downcase: True iff we are required to downcase the field.
    """
    MANDATORY_METADATA = dict(
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

    def verify_blob_checksum(self, bucket: str, key: str, metadata: typing.Dict[str, str]) -> bool:
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


class FileMetadata:
    FILE_FORMAT_VERSION = "0.0.3"

    FORMAT = "format"
    BUNDLE_UUID = "bundle_uuid"
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
