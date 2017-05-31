from __future__ import absolute_import, division, print_function, unicode_literals

from google.cloud.storage import Client
from google.cloud import exceptions

from . import BlobStore, BlobContainerNotFoundError, BlobStoreCredentialError


class GCSBlobStore(BlobStore):
    def __init__(self, container: str, json_keyfile: str) -> None:
        super(GCSBlobStore, self).__init__()

        self.gcs_client = Client.from_service_account_json(json_keyfile)

        try:
            self.bucket = self.gcs_client.get_bucket(container)
        except exceptions.NotFound as e:
            raise BlobContainerNotFoundError(e)
        except exceptions.Forbidden as e:
            raise BlobStoreCredentialError(e)
