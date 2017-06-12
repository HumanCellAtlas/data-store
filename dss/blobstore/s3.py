from __future__ import absolute_import, division, print_function, unicode_literals

import typing

import boto3
import botocore

from . import BlobStore, BlobContainerNotFoundError, BlobStoreCredentialError


class S3BlobStore(BlobStore):
    def __init__(self) -> None:
        super(S3BlobStore, self).__init__()

        # verify that the credentials are valid.
        try:
            boto3.client('sts').get_caller_identity()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "InvalidClientTokenId":
                raise BlobStoreCredentialError()

        self.s3_client = boto3.client("s3")

    def list(self, prefix: str=None):
        pass

    def get_presigned_url(self, objname: str):
        pass

    def set(self, objname: str, src_file_handle: typing.BinaryIO):
        pass

    def delete(self, container: str, object_name: str):
        self.s3_client.delete_object(
            Bucket=container,
            key=object_name
        )

    def get_metadata(self, container: str, object_name: str):
        """
        Retrieves the metadata for a given object in a given container.  If the
        platform has any mandatory prefixes or suffixes for the metadata keys,
        it should be stripped before returned.
        :param container: the container the object resides in.
        :param object_name: the name of the object for which metadata is being
        retrieved.
        :return: a dictionary mapping metadata keys to metadata values.
        """
        response = self.s3_client.head_object(
            Bucket=container,
            Key=object_name
        )
        return response['Metadata']
