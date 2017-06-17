from __future__ import absolute_import, division, print_function, unicode_literals

import typing

from google.cloud.storage import Client
from google.cloud.storage.bucket import Bucket

from . import BlobNotFoundError, BlobStore


class GCSBlobStore(BlobStore):
    def __init__(self, json_keyfile: str) -> None:
        super(GCSBlobStore, self).__init__()

        self.gcs_client = Client.from_service_account_json(json_keyfile)
        self.bucket_map = dict()  # type: typing.MutableMapping[str, Bucket]

    def _ensure_bucket_loaded(self, bucket: str):
        cached_bucket_obj = self.bucket_map.get(bucket, None)
        if cached_bucket_obj is not None:
            return cached_bucket_obj
        bucket_obj = self.gcs_client.bucket(bucket)  # type: Bucket
        self.bucket_map[bucket] = bucket_obj
        return bucket_obj

    def get_metadata(self, bucket: str, object_name: str):
        """
        Retrieves the metadata for a given object in a given bucket.  If the
        platform has any mandatory prefixes or suffixes for the metadata keys,
        they should be stripped before being returned.
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
