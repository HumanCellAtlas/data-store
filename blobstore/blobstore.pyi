from typing import *

class BlobStore(object):
    """Abstract base class for all blob stores."""
    def __init__(self): ...

    def list(self, prefix: str): ...

    def get_presigned_url(self, objname: str): ...

    def set(self, objname: str, src_file_handle: object): ...

    def delete(self, objname: str): ...


class BlobStoreError(Exception): ...
class BlobStoreCredentialError(BlobStoreError): ...
class BlobContainerNotFoundError(BlobStoreError): ...
class BlobNotFoundError(BlobStoreError): ...
class BlobAlreadyExistsError(BlobStoreError): ...
