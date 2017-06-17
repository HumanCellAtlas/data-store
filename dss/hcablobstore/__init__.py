from __future__ import absolute_import, division, print_function, unicode_literals

class HCABlobStore(object):
    """Abstract base class for all HCA-specific logic for dealing with individual clouds."""
    COPIED_METADATA = (
        "hca-dss-sha1",
        "hca-dss-crc32c",
        "hca-dss-sha256",
        "hca-dss-s3_etag",
        "hca-dss-content-type",
    )

    def copy_blob_from_staging(
        self,
        src_bucket: str, src_object_name: str,
        dst_bucket: str, dst_object_name: str,
    ):
        raise NotImplementedError()
