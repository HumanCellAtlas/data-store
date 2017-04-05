from __future__ import absolute_import, division, print_function, unicode_literals

class BlobStore(object):
    """Abstract base class for all blob stores."""
    def __init__(self):
        pass

    def list(self, prefix=None):
        """Returns a list of all blob entries in a container that match a given prefix."""
        raise NotImplementedError()

    def get_presigned_url(self, objname):
        """Retrieves a presigned url for blob objname.  Raises BlobNotFoundError if the blob is not present."""
        raise NotImplementedError()

    def set(self, objname, src_file_handle):
        """Reads from src_file_handle and populate a blob  a blob objname and writes the content to out_file_handle.
        Raises BlobNotFoundError if the blob is not present."""
        raise NotImplementedError()

    def delete(self, objname):
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
