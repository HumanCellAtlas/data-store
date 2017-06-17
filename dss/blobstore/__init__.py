from __future__ import absolute_import, division, print_function, unicode_literals

import typing


class BlobStore(object):
    """Abstract base class for all blob stores."""
    def __init__(self):
        pass

    def list(self, prefix: str=None):
        """Returns a list of all blob entries in a bucket that match a given prefix."""
        raise NotImplementedError()

    def generate_presigned_url(
            self,
            bucket: str,
            object_name: str,
            method: str,
            **kwargs):
        # TODO: things like http ranges need to be explicit parameters.
        # users of this API should not need to know the argument names presented
        # to the cloud API.
        """
        Retrieves a presigned URL for the given HTTP method for blob
        ``object_name``. Raises BlobNotFoundError if the blob is not
        present.
        """
        raise NotImplementedError()

    def upload_file_handle(
            self,
            bucket: str,
            object_name: str,
            src_file_handle: typing.BinaryIO):
        """
        Saves the contents of a file handle as the contents of an object in a
        bucket.
        """
        raise NotImplementedError()

    def delete(self, bucket: str, object_name: str):
        """
        Deletes an object in a bucket.  If the operation definitely did not
        delete anything, return False.  Any other return value is treated as
        something was possibly deleted.
        """
        raise NotImplementedError()

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
        raise NotImplementedError()

    def copy(
            self,
            src_bucket: str, src_object_name: str,
            dst_bucket: str, dst_object_name: str,
            **kwargs):
        raise NotImplementedError()


class BlobStoreError(Exception):
    pass


class BlobStoreUnknownError(BlobStoreError):
    pass


class BlobStoreCredentialError(BlobStoreError):
    pass


class BlobBucketNotFoundError(BlobStoreError):
    pass


class BlobNotFoundError(BlobStoreError):
    pass


class BlobAlreadyExistsError(BlobStoreError):
    pass
