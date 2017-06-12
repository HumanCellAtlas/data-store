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

    def delete(self, objname: str):
        pass
