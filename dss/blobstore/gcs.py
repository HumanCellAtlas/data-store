from __future__ import absolute_import, division, print_function, unicode_literals

from google.cloud.storage import Client
from google.cloud import exceptions

from . import BlobStore, BlobContainerNotFoundError, BlobStoreCredentialError


class GCSBlobStore(BlobStore):
    def __init__(self, json_keyfile: str) -> None:
        super(GCSBlobStore, self).__init__()

        self.gcs_client = Client.from_service_account_json(json_keyfile)
