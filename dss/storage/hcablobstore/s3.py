import typing

from . import HCABlobStore


class S3HCABlobStore(HCABlobStore):
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
        checksum = self.handle.get_cloud_checksum(bucket, key)
        metadata_checksum_key = typing.cast(str, HCABlobStore.MANDATORY_STAGING_METADATA['S3_ETAG']['keyname'])
        return checksum.lower() == metadata[metadata_checksum_key].lower()
