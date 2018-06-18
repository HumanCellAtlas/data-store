import typing

from . import HCABlobStore


class GSHCABlobStore(HCABlobStore):
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
        metadata_checksum_key = typing.cast(str, HCABlobStore.MANDATORY_STAGING_METADATA['CRC32C']['keyname'])
        return checksum.lower() == metadata[metadata_checksum_key].lower()

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
        checksum = self.handle.get_cloud_checksum(bucket, key)
        return checksum.lower() == dss_metadata["crc32c"].lower()
