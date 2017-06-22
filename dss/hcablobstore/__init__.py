from __future__ import absolute_import, division, print_function, unicode_literals


class HCABlobStore(object):
    """Abstract base class for all HCA-specific logic for dealing with individual clouds."""
    MANDATORY_METADATA = dict(
        SHA1="hca-dss-sha1",
        CRC32C="hca-dss-crc32c",
        SHA256="hca-dss-sha256",
        S3_ETAG="hca-dss-s3_etag",
        CONTENT_TYPE="hca-dss-content-type",
    )

    def copy_blob_from_staging(
        self,
        src_bucket: str, src_object_name: str,
        dst_bucket: str, dst_object_name: str,
    ):
        raise NotImplementedError()


class FileMetadata(object):
    FILE_FORMAT_VERSION = "0.0.2"

    FORMAT = "format"
    BUNDLE_UUID = "bundle_uuid"
    CREATOR_UID = "creator_uid"
    VERSION = "version"
    CONTENT_TYPE = "content-type"
    CRC32C = "crc32c"
    S3_ETAG = "s3-etag"
    SHA1 = "sha1"
    SHA256 = "sha256"


class BundleMetadata(object):
    FILE_FORMAT_VERSION = "0.0.1"

    FORMAT = "format"
    CREATOR_UID = "creator_uid"
    VERSION = "version"
    FILES = "files"


class BundleFileMetadata(object):
    NAME = "name"
    UUID = "uuid"
    VERSION = "version"
    CONTENT_TYPE = "content-type"
    INDEXED = "indexed"
    CRC32C = "crc32c"
    S3_ETAG = "s3-etag"
    SHA1 = "sha1"
    SHA256 = "sha256"
