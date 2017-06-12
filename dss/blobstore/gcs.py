from __future__ import absolute_import, division, print_function, unicode_literals

import typing

from google.cloud.storage import Client
from google.cloud.storage.bucket import Bucket

from . import BlobStore


class GCSBlobStore(BlobStore):
    def __init__(self, json_keyfile: str) -> None:
        super(GCSBlobStore, self).__init__()

        self.gcs_client = Client.from_service_account_json(json_keyfile)
        self.container_map = dict()  # type: typing.MutableMapping[str, Bucket]

    def _ensure_container_loaded(self, container: str):
        cached_container_obj = self.container_map.get(container, None)
        if cached_container_obj is not None:
            return cached_container_obj
        container_obj = self.gcs_client.bucket(container)  # type: Bucket
        self.container_map[container] = container_obj
        return container_obj

    def get_metadata(self, container: str, object_name: str):
        """
        Retrieves the metadata for a given object in a given container.  If the
        platform has any mandatory prefixes or suffixes for the metadata keys,
        they should be stripped before being returned.
        :param container: the container the object resides in.
        :param object_name: the name of the object for which metadata is being
        retrieved.
        :return: a dictionary mapping metadata keys to metadata values.
        """
        container_obj = self._ensure_container_loaded(container)
        response = container_obj.get_blob(object_name)
        return response.metadata
