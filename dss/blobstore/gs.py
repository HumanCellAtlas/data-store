import base64
import binascii
import datetime
import typing

from google.cloud.storage import Client
from google.cloud.storage.bucket import Bucket

from . import BlobNotFoundError, BlobStore


class GSBlobStore(BlobStore):
    def __init__(self, json_keyfile: str) -> None:
        super(GSBlobStore, self).__init__()

        self.gcp_client = Client.from_service_account_json(json_keyfile)
        self.bucket_map = dict()  # type: typing.MutableMapping[str, Bucket]

    def _ensure_bucket_loaded(self, bucket: str):
        cached_bucket_obj = self.bucket_map.get(bucket, None)
        if cached_bucket_obj is not None:
            return cached_bucket_obj
        bucket_obj = self.gcp_client.bucket(bucket)  # type: Bucket
        self.bucket_map[bucket] = bucket_obj
        return bucket_obj

    def list(
            self,
            bucket: str,
            prefix: str=None,
            delimiter: str=None,
    ) -> typing.Iterator[str]:
        """
        Returns an iterator of all blob entries in a bucket that match a given prefix.  Do not return any keys that
        contain the delimiter past the
        prefix.
        """
        kwargs = dict()
        if prefix is not None:
            kwargs['prefix'] = prefix
        if delimiter is not None:
            kwargs['delimiter'] = delimiter
        bucket_obj = self._ensure_bucket_loaded(bucket)
        for blob_obj in bucket_obj.list_blobs(**kwargs):
            yield blob_obj.name

    def generate_presigned_GET_url(
            self,
            bucket: str,
            object_name: str,
            **kwargs) -> str:
        bucket_obj = self._ensure_bucket_loaded(bucket)
        blob_obj = bucket_obj.get_blob(object_name)
        return blob_obj.generate_signed_url(datetime.timedelta(days=1))

    def upload_file_handle(
            self,
            bucket: str,
            object_name: str,
            src_file_handle: typing.BinaryIO):
        bucket_obj = self._ensure_bucket_loaded(bucket)
        blob_obj = bucket_obj.blob(object_name, chunk_size=1 * 1024 * 1024)
        blob_obj.upload_from_file(src_file_handle)

    def get(self, bucket: str, object_name: str) -> bytes:
        """
        Retrieves the data for a given object in a given bucket.
        :param bucket: the bucket the object resides in.
        :param object_name: the name of the object for which metadata is being
        retrieved.
        :return: the data
        """
        bucket_obj = self._ensure_bucket_loaded(bucket)
        blob_obj = bucket_obj.get_blob(object_name)
        if blob_obj is None:
            raise BlobNotFoundError()

        return blob_obj.download_as_string()

    def get_cloud_checksum(
            self,
            bucket: str,
            object_name: str
    ) -> str:
        """
        Retrieves the cloud-provided checksum for a given object in a given bucket.
        :param bucket: the bucket the object resides in.
        :param object_name: the name of the object for which checksum is being retrieved.
        :return: the cloud-provided checksum
        """
        bucket_obj = self._ensure_bucket_loaded(bucket)
        blob_obj = bucket_obj.get_blob(object_name)
        if blob_obj is None:
            raise BlobNotFoundError()

        return binascii.hexlify(base64.b64decode(blob_obj.crc32c)).decode("utf-8").lower()

    def get_user_metadata(
            self,
            bucket: str,
            object_name: str
    ) -> typing.Dict[str, str]:
        """
        Retrieves the user metadata for a given object in a given bucket.  If the platform has any mandatory prefixes or
        suffixes for the metadata keys, they should be stripped before being returned.
        :param bucket: the bucket the object resides in.
        :param object_name: the name of the object for which metadata is being
        retrieved.
        :return: a dictionary mapping metadata keys to metadata values.
        """
        bucket_obj = self._ensure_bucket_loaded(bucket)
        response = bucket_obj.get_blob(object_name)
        if response is None:
            raise BlobNotFoundError()
        return response.metadata

    def get_size(
            self,
            bucket: str,
            object_name: str
    ) -> int:
        """
        Retrieves the filesize
        :param bucket: the bucket the object resides in.
        :param object_name: the name of the object for which metadata is being
        retrieved.
        :return: integer equal to filesize in bytes
        """
        bucket_obj = self._ensure_bucket_loaded(bucket)
        response = bucket_obj.get_blob(object_name)
        if response is None:
            raise BlobNotFoundError()
        res = response.size
        return res

    def copy(
            self,
            src_bucket: str, src_object_name: str,
            dst_bucket: str, dst_object_name: str,
            **kwargs
    ):
        src_bucket_obj = self._ensure_bucket_loaded(src_bucket)
        src_blob_obj = src_bucket_obj.get_blob(src_object_name)
        dst_bucket_obj = self._ensure_bucket_loaded(dst_bucket)
        src_bucket_obj.copy_blob(src_blob_obj, dst_bucket_obj, new_name=dst_object_name)
