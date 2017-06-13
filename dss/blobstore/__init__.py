from __future__ import absolute_import, division, print_function, unicode_literals

class BlobStore(object):
    """Abstract base class for all blob stores."""
    def __init__(self):
        pass

    def list(self, prefix: str=None):
        """Returns a list of all blob entries in a container that match a given prefix."""
        raise NotImplementedError()

    def generate_presigned_url(self, object_name: str, method: str):
        """
        Retrieves a presigned URL for the given HTTP method for blob
        ``object_name``. Raises BlobNotFoundError if the blob is not
        present.
        """
        raise NotImplementedError()

    def delete(self, container: str, object_name: str):
        """
        Deletes an object in a container.  If the operation definitely did not
        delete anything, return False.  Any other return value is treated as
        something was possibly deleted.
        """
        raise NotImplementedError()


class BlobStoreError(Exception):
    pass


class BlobStoreCredentialError(BlobStoreError):
    pass


class BlobContainerNotFoundError(BlobStoreError):
    pass


class BlobNotFoundError(BlobStoreError):
    pass


class BlobAlreadyExistsError(BlobStoreError):
    pass
