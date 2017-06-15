from __future__ import absolute_import, division, print_function, unicode_literals

import typing


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

    def upload_file_handle(
            self,
            container: str,
            object_name: str,
            src_file_handle: typing.BinaryIO):
        """
        Saves the contents of a file handle as the contents of an object in a
        container.
        """
        raise NotImplementedError()

    def delete(self, container: str, object_name: str):
        """
        Deletes an object in a container.  If the operation definitely did not
        delete anything, return False.  Any other return value is treated as
        something was possibly deleted.
        """
        raise NotImplementedError()

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
        raise NotImplementedError()

    def copy(
            self,
            src_container: str, src_object_name: str,
            dst_container: str, dst_object_name: str,
            **kwargs):
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
